package diskqueue

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// logging stuff copied from github.com/nsqio/nsq/internal/lg

type LogLevel int

const (
	DEBUG = LogLevel(1)
	INFO  = LogLevel(2)
	WARN  = LogLevel(3)
	ERROR = LogLevel(4)
	FATAL = LogLevel(5)
)

type AppLogFunc func(lvl LogLevel, f string, args ...interface{})

func (l LogLevel) String() string {
	switch l {
	case 1:
		return "DEBUG"
	case 2:
		return "INFO"
	case 3:
		return "WARNING"
	case 4:
		return "ERROR"
	case 5:
		return "FATAL"
	}
	panic("invalid LogLevel")
}

// Errors returned by batch-aware queue operations.
var (
	// ErrExiting is returned when an operation is attempted against a
	// closed (or closing) queue.
	ErrExiting = errors.New("queue is exiting")
	// ErrEmptyBatch is returned when PutBatch is called with no messages.
	ErrEmptyBatch = errors.New("batch must contain at least one message")
	// ErrDuplicateBatch is returned when a PutBatch ID has already been
	// committed (or is older than the last committed batch ID).
	ErrDuplicateBatch = errors.New("batch ID already committed")
	// ErrBatchCommit is returned when a batch could not be durably
	// committed. The partial batch has been rolled back and the queue
	// still contains every previously committed message.
	ErrBatchCommit = errors.New("batch commit failed and was rolled back")
)

// frame magic values. They are negative int32 values, outside the range of
// valid message size headers (which are always non-negative), so they can
// never collide with legacy data frames.
const (
	// "BAT1": followed by uint64 batch ID and int32 message count (12 bytes).
	batchBeginMagic int32 = -0x42415431
	// "BAT2": followed by uint64 batch ID, int32 message count and a
	// uint32 CRC32 checksum of the begin frame payload (16 bytes).
	batchCommitMagic int32 = -0x42415432
)

const (
	batchBeginFrameLen  = 4 + 12
	batchCommitFrameLen = 4 + 16
)

// errIncompleteBatch is an internal sentinel indicating that a reader
// encountered a batch begin frame without a matching, valid commit frame.
var errIncompleteBatch = errors.New("incomplete batch")

// errEOFAtFrameBoundary signals a clean end-of-file reached exactly on a
// frame boundary: the current file holds no more readable bytes and the next
// frame (if any) lives in the following file. It is an expected rotation
// condition, not corruption, and must never produce a .bad file.
var errEOFAtFrameBoundary = errors.New("EOF at frame boundary")

type Interface interface {
	Put([]byte) error
	ReadChan() <-chan []byte // this is expected to be an *unbuffered* channel
	PeekChan() <-chan []byte // this is expected to be an *unbuffered* channel
	Close() error
	Delete() error
	Depth() int64
	Empty() error
}

// BatchInterface is an Interface that additionally supports atomic,
// recoverable multi-message batches.
type BatchInterface interface {
	Interface

	// PutBatch writes a group of related messages as one atomic unit.
	//
	// id is a stable, producer-chosen batch identifier used to reject
	// retried duplicates; data is the ordered list of member messages.
	// The batch only becomes visible to readers once every member frame
	// and the commit tail record have been fsync'd together. A failed
	// commit rolls the partial batch back, leaving previously committed
	// data untouched.
	PutBatch(id uint64, data [][]byte) error
}

// readyMsg is a buffered message plus the read cursor that must be
// persisted once the message is delivered (not merely peeked at).
type readyMsg struct {
	data []byte
	// cursor positioned immediately after this message's frame
	nextFileNum int64
	nextPos     int64
	// true for the final member of a committed batch
	endOfBatch bool
}

// diskQueue implements a filesystem backed FIFO queue
type diskQueue struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms

	// run-time state (also persisted to disk)
	readPos      int64
	writePos     int64
	readFileNum  int64
	writeFileNum int64
	depth        int64

	// high-water mark of durably committed data. Readers may only
	// advance up to this cursor; frames of an in-flight batch live
	// between here and writePos and are invisible until commit.
	committedFileNum int64
	committedPos     int64

	sync.RWMutex

	// instantiation time metadata
	name                string
	dataPath            string
	maxBytesPerFile     int64 // cannot change once created
	maxBytesPerFileRead int64
	minMsgSize          int32
	maxMsgSize          int32
	syncEvery           int64         // number of writes per fsync
	syncTimeout         time.Duration // duration of time per fsync
	exitFlag            int32
	needSync            bool

	// batch bookkeeping
	lastBatchID       uint64
	committedBatchIDs map[uint64]struct{}

	// resumeRemaining is non-zero when the read cursor was restored into
	// the middle of a committed batch (after a restart): it counts the
	// member frames still to deliver before the commit tail is consumed.
	resumeRemaining int32

	// keeps track of the position where we have read
	// (but not yet sent over readChan)
	nextReadPos     int64
	nextReadFileNum int64

	readFile  *os.File
	writeFile *os.File
	reader    *bufio.Reader
	writeBuf  bytes.Buffer

	// messages read ahead but not yet delivered (a whole committed batch
	// is buffered together, single Puts buffer one message)
	readAhead []readyMsg

	// exposed via ReadChan()
	readChan chan []byte

	// exposed via PeekChan()
	peekChan chan []byte

	// internal channels
	depthChan         chan int64
	writeChan         chan []byte
	writeResponseChan chan error
	batchChan         chan batchRequest
	emptyChan         chan int
	emptyResponseChan chan error
	exitChan          chan int
	exitSyncChan      chan int

	// syncErrHook, when non-nil, is consulted by sync(); a non-nil return
	// simulates an fsync failure. It exists for tests.
	syncErrHook func() error

	logf AppLogFunc
}

type batchRequest struct {
	id   uint64
	data [][]byte
}

// New instantiates an instance of diskQueue, retrieving metadata
// from the filesystem and starting the read ahead goroutine
func New(name string, dataPath string, maxBytesPerFile int64,
	minMsgSize int32, maxMsgSize int32,
	syncEvery int64, syncTimeout time.Duration, logf AppLogFunc) Interface {
	d := diskQueue{
		name:              name,
		dataPath:          dataPath,
		maxBytesPerFile:   maxBytesPerFile,
		minMsgSize:        minMsgSize,
		maxMsgSize:        maxMsgSize,
		readChan:          make(chan []byte),
		peekChan:          make(chan []byte),
		depthChan:         make(chan int64),
		writeChan:         make(chan []byte),
		writeResponseChan: make(chan error),
		batchChan:         make(chan batchRequest),
		emptyChan:         make(chan int),
		emptyResponseChan: make(chan error),
		exitChan:          make(chan int),
		exitSyncChan:      make(chan int),
		syncEvery:         syncEvery,
		syncTimeout:       syncTimeout,
		logf:              logf,
		committedBatchIDs: make(map[uint64]struct{}),
	}

	// no need to lock here, nothing else could possibly be touching this instance
	err := d.retrieveMetaData()
	if err != nil && !os.IsNotExist(err) {
		d.logf(ERROR, "DISKQUEUE(%s) failed to retrieveMetaData - %s", d.name, err)
	}

	// recover from any crash: discard half-written batches, salvage the
	// committed tail and reconcile depth, cursors and batch IDs
	if err := d.recoverBatches(); err != nil {
		d.logf(ERROR, "DISKQUEUE(%s) failed to recover batches - %s", d.name, err)
	}

	go d.ioLoop()
	return &d
}

// Depth returns the depth of the queue
func (d *diskQueue) Depth() int64 {
	depth, ok := <-d.depthChan
	if !ok {
		// ioLoop exited
		depth = d.depth
	}
	return depth
}

// ReadChan returns the receive-only []byte channel for reading data
func (d *diskQueue) ReadChan() <-chan []byte {
	return d.readChan
}

func (d *diskQueue) PeekChan() <-chan []byte {
	return d.peekChan
}

// Put writes a []byte to the queue
func (d *diskQueue) Put(data []byte) error {
	d.RLock()
	defer d.RUnlock()

	if d.exitFlag == 1 {
		return ErrExiting
	}

	select {
	case d.writeChan <- data:
		return <-d.writeResponseChan
	case <-d.exitChan:
		return ErrExiting
	}
}

// PutBatch writes a group of messages as one atomic, recoverable batch.
func (d *diskQueue) PutBatch(id uint64, data [][]byte) error {
	d.RLock()
	defer d.RUnlock()

	if d.exitFlag == 1 {
		return ErrExiting
	}

	select {
	case d.batchChan <- batchRequest{id: id, data: data}:
		return <-d.writeResponseChan
	case <-d.exitChan:
		return ErrExiting
	}
}

// Close cleans up the queue and persists metadata
func (d *diskQueue) Close() error {
	err := d.exit(false)
	if err != nil {
		return err
	}
	return d.sync()
}

func (d *diskQueue) Delete() error {
	return d.exit(true)
}

func (d *diskQueue) exit(deleted bool) error {
	d.Lock()
	if d.exitFlag == 1 {
		d.Unlock()
		return ErrExiting
	}
	d.exitFlag = 1
	d.Unlock()

	if deleted {
		d.logf(INFO, "DISKQUEUE(%s): deleting", d.name)
	} else {
		d.logf(INFO, "DISKQUEUE(%s): closing", d.name)
	}

	close(d.exitChan)
	// ensure that ioLoop has exited
	<-d.exitSyncChan

	// closing depthChan is safe: every Close/Delete caller now gets the
	// cached depth from Depth() instead of blocking
	d.closeDepthChan()

	if d.readFile != nil {
		d.readFile.Close()
		d.readFile = nil
	}

	if d.writeFile != nil {
		d.writeFile.Close()
		d.writeFile = nil
	}

	return nil
}

// closeDepthChan closes depthChan at most once; concurrent Close callers
// (all but the first return ErrExiting before reaching here) cannot race it.
func (d *diskQueue) closeDepthChan() {
	defer func() { recover() }()
	close(d.depthChan)
}

// Empty destructively clears out any pending data in the queue
// by fast forwarding read positions and removing intermediate files
func (d *diskQueue) Empty() error {
	d.RLock()
	defer d.RUnlock()

	if d.exitFlag == 1 {
		return ErrExiting
	}

	d.logf(INFO, "DISKQUEUE(%s): emptying", d.name)

	select {
	case d.emptyChan <- 1:
		return <-d.emptyResponseChan
	case <-d.exitChan:
		return ErrExiting
	}
}

func (d *diskQueue) deleteAllFiles() error {
	err := d.skipToNextRWFile()

	innerErr := os.Remove(d.metaDataFileName())
	if innerErr != nil && !os.IsNotExist(innerErr) {
		d.logf(ERROR, "DISKQUEUE(%s) failed to remove metadata file - %s", d.name, innerErr)
		return innerErr
	}

	return err
}

func (d *diskQueue) skipToNextRWFile() error {
	var err error

	if d.readFile != nil {
		d.readFile.Close()
		d.readFile = nil
	}

	if d.writeFile != nil {
		d.writeFile.Close()
		d.writeFile = nil
	}

	for i := d.readFileNum; i <= d.writeFileNum; i++ {
		fn := d.fileName(i)
		innerErr := os.Remove(fn)
		if innerErr != nil && !os.IsNotExist(innerErr) {
			d.logf(ERROR, "DISKQUEUE(%s) failed to remove data file - %s", d.name, innerErr)
			err = innerErr
		}
	}

	d.writeFileNum++
	d.writePos = 0
	d.readFileNum = d.writeFileNum
	d.readPos = 0
	d.nextReadFileNum = d.writeFileNum
	d.nextReadPos = 0
	d.committedFileNum = d.writeFileNum
	d.committedPos = 0
	d.depth = 0
	d.readAhead = nil
	d.resumeRemaining = 0

	return err
}

// openReadFileAt opens (or re-opens) the read file at the requested cursor,
// initializing maxBytesPerFileRead from the file size for completed files.
func (d *diskQueue) openReadFileAt(fileNum, pos int64) error {
	if d.readFile != nil {
		d.readFile.Close()
		d.readFile = nil
	}

	curFileName := d.fileName(fileNum)
	f, err := os.OpenFile(curFileName, os.O_RDONLY, 0600)
	if err != nil {
		return err
	}

	d.logf(INFO, "DISKQUEUE(%s): openReadFileAt() opened %s", d.name, curFileName)

	if pos > 0 {
		if _, err = f.Seek(pos, 0); err != nil {
			f.Close()
			return err
		}
	}

	// for "complete" files (i.e. not the "current" file), maxBytesPerFileRead
	// should be initialized to the file's size, or default to maxBytesPerFile
	d.maxBytesPerFileRead = d.maxBytesPerFile
	if fileNum < d.writeFileNum {
		stat, statErr := f.Stat()
		if statErr == nil {
			d.maxBytesPerFileRead = stat.Size()
		}
	}

	d.readFile = f
	d.reader = bufio.NewReader(f)
	return nil
}

func (d *diskQueue) closeReadFile() {
	if d.readFile != nil {
		d.readFile.Close()
		d.readFile = nil
	}
}

// atFrameBoundaryEOFAt reports whether the cursor (fileNum, pos) sits exactly
// at the physical end of its file while committed data still exists beyond
// it, i.e. the next frame must be read from the following file.
func (d *diskQueue) atFrameBoundaryEOFAt(fileNum, pos int64) bool {
	info, err := os.Stat(d.fileName(fileNum))
	if err != nil || pos != info.Size() {
		return false
	}
	return fileNum < d.committedFileNum ||
		(fileNum == d.committedFileNum && pos < d.committedPos)
}

// advanceToNextFile moves the read cursor to the start of the next file
// after a clean frame-boundary EOF. Unlike handleReadError it does not
// quarantine the current file.
func (d *diskQueue) advanceToNextFile() {
	d.closeReadFile()
	d.readFileNum++
	d.readPos = 0
	d.nextReadFileNum = d.readFileNum
	d.nextReadPos = 0
	d.needSync = true
}

// advanceCursor returns the read cursor after consuming totalBytes at
// (fileNum, pos), applying the same rotation rule as the historical
// single-message read path.
//
// The threshold is the file's real size for a completed (already rotated)
// file, and maxBytesPerFile for the file still being written. This mirrors
// the write-side invariant that a file only rolls when the next frame does
// not fit within maxBytesPerFile.
func (d *diskQueue) advanceCursor(fileNum, pos, total int64) (int64, int64) {
	nextFileNum, nextPos := fileNum, pos+total

	limit := d.maxBytesPerFile
	if fileNum < d.writeFileNum {
		if info, err := os.Stat(d.fileName(fileNum)); err == nil {
			limit = info.Size()
		}
	}

	if fileNum <= d.writeFileNum && nextPos >= limit {
		d.closeReadFile()
		nextFileNum++
		nextPos = 0
	}
	return nextFileNum, nextPos
}

// prefetch reads the next deliverable unit starting at the read cursor:
// either a single legacy message or an entire committed batch. The returned
// readyMsg slice carries the per-message delivery cursors.
func (d *diskQueue) prefetch() ([]readyMsg, error) {
	if d.readFile == nil {
		if err := d.openReadFileAt(d.readFileNum, d.readPos); err != nil {
			return nil, err
		}
	}

	var frameSize int32
	if err := binary.Read(d.reader, binary.BigEndian, &frameSize); err != nil {
		d.closeReadFile()
		if err == io.EOF && d.atFrameBoundaryEOFAt(d.readFileNum, d.readPos) {
			return nil, errEOFAtFrameBoundary
		}
		return nil, err
	}

	if frameSize == batchBeginMagic {
		return d.prefetchBatch()
	}

	// the read cursor was restored into the middle of a committed batch;
	// consume only the remaining members then the commit tail
	if d.resumeRemaining > 0 {
		return d.prefetchBatchRemaining(d.resumeRemaining, frameSize)
	}

	if frameSize < d.minMsgSize || frameSize > d.maxMsgSize {
		// this file is corrupt and we have no reasonable guarantee on
		// where a new message should begin
		d.closeReadFile()
		return nil, fmt.Errorf("invalid message read size (%d)", frameSize)
	}

	readBuf := make([]byte, frameSize)
	if _, err := io.ReadFull(d.reader, readBuf); err != nil {
		d.closeReadFile()
		return nil, err
	}

	totalBytes := int64(4 + frameSize)
	nextFileNum, nextPos := d.advanceCursor(d.readFileNum, d.readPos, totalBytes)

	// we only advance next* because we have not yet sent this to consumers
	// (where readFileNum, readPos will actually be advanced)
	d.nextReadFileNum = nextFileNum
	d.nextReadPos = nextPos

	return []readyMsg{{
		data:        readBuf,
		nextFileNum: nextFileNum,
		nextPos:     nextPos,
	}}, nil
}

// prefetchBatch reads an entire committed batch starting right after the
// begin magic that the caller has already consumed. Any structural problem
// (missing commit, truncated member, checksum mismatch) means the batch was
// never committed and is reported as errIncompleteBatch so the salvage path
// discards it wholesale.
func (d *diskQueue) prefetchBatch() ([]readyMsg, error) {
	beginPayload := make([]byte, 12)
	if _, err := io.ReadFull(d.reader, beginPayload); err != nil {
		d.closeReadFile()
		return nil, errIncompleteBatch
	}
	batchID := binary.BigEndian.Uint64(beginPayload[:8])
	count := int32(binary.BigEndian.Uint32(beginPayload[8:]))
	if count <= 0 {
		d.closeReadFile()
		return nil, errIncompleteBatch
	}

	cursorFile := d.readFileNum
	cursorPos := d.readPos + batchBeginFrameLen
	msgs, cursorFile, cursorPos, err := d.readBatchMembers(cursorFile, cursorPos, count, count)
	if err != nil {
		return nil, err
	}

	commitEndFile, commitEndPos, cerr := d.consumeCommit(batchID, count, beginPayload, cursorFile, cursorPos)
	if cerr != nil {
		return nil, cerr
	}

	// the last member's delivery cursor must land *after* the commit tail
	// record, not merely after the last data frame; otherwise a restart (or
	// the next prefetch) would reopen on the commit frame and treat it as
	// corrupt data
	msgs[len(msgs)-1].nextFileNum = commitEndFile
	msgs[len(msgs)-1].nextPos = commitEndPos

	// nothing may be delivered past the commit frame until the batch is
	// fully buffered, so next* may jump straight to its end
	d.nextReadFileNum = commitEndFile
	d.nextReadPos = commitEndPos

	return msgs, nil
}

// prefetchBatchRemaining resumes a committed batch whose earlier members
// were already delivered before a restart. firstFrameSize is the size header
// prefetch() just consumed at the read cursor; remaining counts all member
// frames still to deliver (the current one included).
func (d *diskQueue) prefetchBatchRemaining(remaining, firstFrameSize int32) ([]readyMsg, error) {
	if remaining <= 0 || firstFrameSize < d.minMsgSize || firstFrameSize > d.maxMsgSize {
		d.closeReadFile()
		return nil, errIncompleteBatch
	}

	cursorFile := d.readFileNum
	cursorPos := d.readPos
	msgs := make([]readyMsg, 0, remaining)

	// first member: its size header was already consumed by prefetch(),
	// read only the body and compute the cursor past the whole frame
	firstBuf := make([]byte, firstFrameSize)
	if _, err := io.ReadFull(d.reader, firstBuf); err != nil {
		d.closeReadFile()
		return nil, errIncompleteBatch
	}
	firstTotal := int64(4 + firstFrameSize)
	nextFile, nextPos := d.advanceCursor(cursorFile, cursorPos, firstTotal)
	msgs = append(msgs, readyMsg{
		data:        firstBuf,
		nextFileNum: nextFile,
		nextPos:     nextPos,
		endOfBatch:  remaining == 1,
	})
	cursorFile, cursorPos = nextFile, nextPos

	// remaining members after the first
	if remaining > 1 {
		var more []readyMsg
		var moreErr error
		more, cursorFile, cursorPos, moreErr = d.readBatchMembers(cursorFile, cursorPos, remaining-1, remaining-1)
		if moreErr != nil {
			return nil, moreErr
		}
		msgs = append(msgs, more...)
	}

	// commit frame follows; full checksum validation already ran during
	// the startup recovery scan, so only the structure is checked here
	commitEndFile, commitEndPos, cerr := d.consumeCommitResumed(cursorFile, cursorPos)
	if cerr != nil {
		return nil, cerr
	}
	msgs[len(msgs)-1].nextFileNum = commitEndFile
	msgs[len(msgs)-1].nextPos = commitEndPos

	d.resumeRemaining = 0
	d.nextReadFileNum = commitEndFile
	d.nextReadPos = commitEndPos
	return msgs, nil
}

// ensureReaderAt makes sure d.reader is positioned at (fileNum, pos),
// opening the file (and re-opening after a frame-boundary rotation) as
// needed. The buffered reader always reflects the requested cursor because
// callers advance strictly forward through frames.
func (d *diskQueue) ensureReaderAt(fileNum, pos int64) error {
	if d.readFile != nil && d.readFileNum == fileNum {
		return nil
	}
	return d.openReadFileAt(fileNum, pos)
}

// readFrameHeaderAt reads a 4-byte big-endian frame header at the cursor,
// transparently skipping to the next file when the current one ends on a
// clean frame boundary. The returned (fileNum, pos) is the possibly updated
// cursor, still positioned on the header that was read.
func (d *diskQueue) readFrameHeaderAt(fileNum, pos int64) (int32, int64, int64, error) {
	if err := d.ensureReaderAt(fileNum, pos); err != nil {
		return 0, 0, 0, err
	}
	var frameSize int32
	if err := binary.Read(d.reader, binary.BigEndian, &frameSize); err != nil {
		if err == io.EOF && d.readPos == pos && d.readFileNum == fileNum &&
			d.atFrameBoundaryEOFAt(fileNum, pos) {
			d.advanceToNextFile()
			fileNum, pos = d.readFileNum, d.readPos
			if openErr := d.openReadFileAt(fileNum, pos); openErr != nil {
				return 0, 0, 0, openErr
			}
			if readErr := binary.Read(d.reader, binary.BigEndian, &frameSize); readErr != nil {
				d.closeReadFile()
				return 0, 0, 0, readErr
			}
			return frameSize, fileNum, pos, nil
		}
		d.closeReadFile()
		return 0, 0, 0, err
	}
	return frameSize, fileNum, pos, nil
}

// readBatchMembers reads count member frames beginning at (fileNum, pos),
// tagging every readyMsg with endOfBatch on the final one. It returns the
// buffered messages plus the cursor immediately following the last frame.
func (d *diskQueue) readBatchMembers(fileNum, pos int64, count, total int32) ([]readyMsg, int64, int64, error) {
	cursorFile, cursorPos := fileNum, pos
	msgs := make([]readyMsg, 0, count)

	for i := int32(0); i < count; i++ {
		frameSize, hdrFile, hdrPos, hdrErr := d.readFrameHeaderAt(cursorFile, cursorPos)
		if hdrErr != nil {
			return nil, 0, 0, errIncompleteBatch
		}
		cursorFile, cursorPos = hdrFile, hdrPos
		if frameSize < d.minMsgSize || frameSize > d.maxMsgSize {
			d.closeReadFile()
			return nil, 0, 0, errIncompleteBatch
		}

		readBuf := make([]byte, frameSize)
		if _, err := io.ReadFull(d.reader, readBuf); err != nil {
			d.closeReadFile()
			return nil, 0, 0, errIncompleteBatch
		}

		nextFile, nextPos := d.advanceCursor(cursorFile, cursorPos, int64(4+frameSize))
		msgs = append(msgs, readyMsg{
			data:        readBuf,
			nextFileNum: nextFile,
			nextPos:     nextPos,
			endOfBatch:  i == total-1,
		})
		cursorFile, cursorPos = nextFile, nextPos
	}
	return msgs, cursorFile, cursorPos, nil
}

// consumeCommit reads and fully validates the commit tail following a batch
// entered through its begin frame, returning the cursor past the tail.
func (d *diskQueue) consumeCommit(batchID uint64, count int32, beginPayload []byte,
	fileNum, pos int64) (int64, int64, error) {
	commitMagic, hdrFile, hdrPos, hdrErr := d.readFrameHeaderAt(fileNum, pos)
	if hdrErr != nil {
		return 0, 0, errIncompleteBatch
	}
	fileNum, pos = hdrFile, hdrPos
	if commitMagic != batchCommitMagic {
		d.closeReadFile()
		return 0, 0, errIncompleteBatch
	}

	commitPayload := make([]byte, 16)
	if _, err := io.ReadFull(d.reader, commitPayload); err != nil {
		d.closeReadFile()
		return 0, 0, errIncompleteBatch
	}
	commitID := binary.BigEndian.Uint64(commitPayload[:8])
	commitCount := int32(binary.BigEndian.Uint32(commitPayload[8:12]))
	checksum := binary.BigEndian.Uint32(commitPayload[12:])
	if commitID != batchID || commitCount != count ||
		checksum != crc32.ChecksumIEEE(beginPayload) {
		d.closeReadFile()
		return 0, 0, errIncompleteBatch
	}

	endFile, endPos := d.advanceCursor(fileNum, pos, batchCommitFrameLen)
	return endFile, endPos, nil
}

// consumeCommitResumed validates just the commit-tail structure (magic and
// payload length) when resuming mid-batch; full checksum verification was
// performed by the startup recovery scan.
func (d *diskQueue) consumeCommitResumed(fileNum, pos int64) (int64, int64, error) {
	commitMagic, hdrFile, hdrPos, hdrErr := d.readFrameHeaderAt(fileNum, pos)
	if hdrErr != nil {
		return 0, 0, errIncompleteBatch
	}
	fileNum, pos = hdrFile, hdrPos
	if commitMagic != batchCommitMagic {
		d.closeReadFile()
		return 0, 0, errIncompleteBatch
	}
	commitPayload := make([]byte, 16)
	if _, err := io.ReadFull(d.reader, commitPayload); err != nil {
		d.closeReadFile()
		return 0, 0, errIncompleteBatch
	}

	endFile, endPos := d.advanceCursor(fileNum, pos, batchCommitFrameLen)
	return endFile, endPos, nil
}

// rotateWriteFileIfNeeded rolls the write file when frame of totalBytes
// would overflow maxBytesPerFile. The check runs before every frame write,
// so a commit tail record is always written directly after the last member
// of its batch and never stranded across a rotation.
func (d *diskQueue) rotateWriteFileIfNeeded(totalBytes int64) error {
	if d.writePos > 0 && d.writePos+totalBytes > d.maxBytesPerFile {
		if d.readFileNum == d.writeFileNum {
			d.maxBytesPerFileRead = d.writePos
		}

		d.writeFileNum++
		d.writePos = 0

		// sync every time we start writing to a new file
		if err := d.sync(); err != nil {
			d.logf(ERROR, "DISKQUEUE(%s) failed to sync - %s", d.name, err)
		}

		if d.writeFile != nil {
			d.writeFile.Close()
			d.writeFile = nil
		}
	}
	return nil
}

func (d *diskQueue) ensureWriteFile() error {
	if d.writeFile != nil {
		return nil
	}

	curFileName := d.fileName(d.writeFileNum)
	f, err := os.OpenFile(curFileName, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return err
	}

	d.logf(INFO, "DISKQUEUE(%s): ensureWriteFile() opened %s", d.name, curFileName)

	if d.writePos > 0 {
		if _, err = f.Seek(d.writePos, 0); err != nil {
			f.Close()
			return err
		}
	}
	d.writeFile = f
	return nil
}

// writeFrame appends a magic + payload frame (used for batch markers).
func (d *diskQueue) writeFrame(magic int32, payload []byte) error {
	totalBytes := int64(4 + len(payload))
	if err := d.rotateWriteFileIfNeeded(totalBytes); err != nil {
		return err
	}
	if err := d.ensureWriteFile(); err != nil {
		return err
	}

	d.writeBuf.Reset()
	if err := binary.Write(&d.writeBuf, binary.BigEndian, magic); err != nil {
		return err
	}
	if _, err := d.writeBuf.Write(payload); err != nil {
		return err
	}

	// only write to the file once
	if _, err := d.writeFile.Write(d.writeBuf.Bytes()); err != nil {
		d.writeFile.Close()
		d.writeFile = nil
		return err
	}

	d.writePos += totalBytes
	return nil
}

// writeDataFrame appends a legacy length-prefixed message frame.
func (d *diskQueue) writeDataFrame(data []byte) error {
	dataLen := int32(len(data))
	totalBytes := int64(4 + dataLen)

	if dataLen < d.minMsgSize || dataLen > d.maxMsgSize {
		return fmt.Errorf("invalid message write size (%d) minMsgSize=%d maxMsgSize=%d", dataLen, d.minMsgSize, d.maxMsgSize)
	}

	// will not wrap-around if maxBytesPerFile + maxMsgSize < Int64Max
	if err := d.rotateWriteFileIfNeeded(totalBytes); err != nil {
		return err
	}
	if err := d.ensureWriteFile(); err != nil {
		return err
	}

	d.writeBuf.Reset()
	if err := binary.Write(&d.writeBuf, binary.BigEndian, dataLen); err != nil {
		return err
	}
	if _, err := d.writeBuf.Write(data); err != nil {
		return err
	}

	// only write to the file once
	if _, err := d.writeFile.Write(d.writeBuf.Bytes()); err != nil {
		d.writeFile.Close()
		d.writeFile = nil
		return err
	}

	d.writePos += totalBytes
	return nil
}

// writeOne performs a low level filesystem write for a single []byte
// while advancing write positions and rolling files, if necessary.
// A single Put is its own commit: the committed high-water mark advances
// immediately with the physical write position.
func (d *diskQueue) writeOne(data []byte) error {
	if err := d.writeDataFrame(data); err != nil {
		return err
	}
	d.depth += 1
	d.committedFileNum = d.writeFileNum
	d.committedPos = d.writePos
	return nil
}

func batchBeginPayload(id uint64, count int32) []byte {
	b := make([]byte, 12)
	binary.BigEndian.PutUint64(b[:8], id)
	binary.BigEndian.PutUint32(b[8:], uint32(count))
	return b
}

func batchCommitPayload(id uint64, count int32, checksum uint32) []byte {
	b := make([]byte, 16)
	binary.BigEndian.PutUint64(b[:8], id)
	binary.BigEndian.PutUint32(b[8:12], uint32(count))
	binary.BigEndian.PutUint32(b[12:], checksum)
	return b
}

// writeBatch appends a full batch and only makes it visible after the
// member frames plus commit tail record have been synced together.
func (d *diskQueue) writeBatch(id uint64, data [][]byte) error {
	if len(data) == 0 {
		return ErrEmptyBatch
	}
	// a stable ID protects producers retrying a whole batch
	if d.lastBatchID != 0 && id <= d.lastBatchID {
		return ErrDuplicateBatch
	}
	if _, ok := d.committedBatchIDs[id]; ok {
		return ErrDuplicateBatch
	}
	for _, msg := range data {
		msgLen := int32(len(msg))
		if msgLen < d.minMsgSize || msgLen > d.maxMsgSize {
			return fmt.Errorf("invalid message write size (%d) minMsgSize=%d maxMsgSize=%d",
				msgLen, d.minMsgSize, d.maxMsgSize)
		}
	}

	// rollback snapshot: everything recorded here is restored on failure
	savedFileNum := d.writeFileNum
	savedPos := d.writePos
	savedDepth := d.depth
	savedLastBatchID := d.lastBatchID

	rollback := func() {
		d.logf(WARN, "DISKQUEUE(%s): rolling back incomplete batch %d", d.name, id)

		d.closeReadFile()
		if d.writeFile != nil {
			d.writeFile.Close()
			d.writeFile = nil
		}

		// files created by rotations within this batch never existed before it
		for fn := savedFileNum + 1; fn <= d.writeFileNum; fn++ {
			if err := os.Remove(d.fileName(fn)); err != nil && !os.IsNotExist(err) {
				d.logf(ERROR, "DISKQUEUE(%s) failed to remove rolled-back file - %s", d.name, err)
			}
		}

		// trim the partial frames out of the original write file
		if f, err := os.OpenFile(d.fileName(savedFileNum), os.O_RDWR, 0600); err == nil {
			if err := f.Truncate(savedPos); err != nil {
				d.logf(ERROR, "DISKQUEUE(%s) failed to truncate rolled-back file - %s", d.name, err)
			}
			f.Close()
		} else if !os.IsNotExist(err) {
			d.logf(ERROR, "DISKQUEUE(%s) failed to open rolled-back file - %s", d.name, err)
		}

		d.writeFileNum = savedFileNum
		d.writePos = savedPos
		d.committedFileNum = savedFileNum
		d.committedPos = savedPos
		d.depth = savedDepth
		d.lastBatchID = savedLastBatchID
		delete(d.committedBatchIDs, id)
	}

	count := int32(len(data))
	beginPayload := batchBeginPayload(id, count)
	if err := d.writeFrame(batchBeginMagic, beginPayload); err != nil {
		rollback()
		return fmt.Errorf("%w: %s", ErrBatchCommit, err)
	}

	for _, msg := range data {
		if err := d.writeDataFrame(msg); err != nil {
			rollback()
			return fmt.Errorf("%w: %s", ErrBatchCommit, err)
		}
	}

	checksum := crc32.ChecksumIEEE(beginPayload)
	commitPayload := batchCommitPayload(id, count, checksum)
	if err := d.writeFrame(batchCommitMagic, commitPayload); err != nil {
		rollback()
		return fmt.Errorf("%w: %s", ErrBatchCommit, err)
	}

	// all frames are on disk; visibility and depth move together, only
	// after a durable sync of data + metadata
	d.depth += int64(len(data))
	d.lastBatchID = id
	d.committedBatchIDs[id] = struct{}{}
	d.committedFileNum = d.writeFileNum
	d.committedPos = d.writePos
	d.needSync = true

	if err := d.sync(); err != nil {
		d.logf(ERROR, "DISKQUEUE(%s) batch %d commit failed - %s", d.name, id, err)
		rollback()
		return fmt.Errorf("%w: %s", ErrBatchCommit, err)
	}

	return nil
}

// sync fsyncs the current writeFile and persists metadata
func (d *diskQueue) sync() error {
	if d.syncErrHook != nil {
		if err := d.syncErrHook(); err != nil {
			if d.writeFile != nil {
				d.writeFile.Close()
				d.writeFile = nil
			}
			return err
		}
	}

	if d.writeFile != nil {
		err := d.writeFile.Sync()
		if err != nil {
			d.writeFile.Close()
			d.writeFile = nil
			return err
		}
	}

	err := d.persistMetaData()
	if err != nil {
		return err
	}

	d.needSync = false
	return nil
}

// retrieveMetaData initializes state from the filesystem
func (d *diskQueue) retrieveMetaData() error {
	var f *os.File
	var err error

	fileName := d.metaDataFileName()
	f, err = os.OpenFile(fileName, os.O_RDONLY, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	// the metadata format is 3 required lines plus an optional 4th line
	// holding the last committed batch ID
	scanner := bufio.NewScanner(f)
	var lines []string
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	if err = scanner.Err(); err != nil {
		return err
	}
	if len(lines) < 3 {
		return errors.New("corrupt metadata file")
	}

	var depth int64
	if _, err = fmt.Sscanf(lines[0], "%d", &depth); err != nil {
		return err
	}
	if _, err = fmt.Sscanf(lines[1], "%d,%d", &d.readFileNum, &d.readPos); err != nil {
		return err
	}
	if _, err = fmt.Sscanf(lines[2], "%d,%d", &d.writeFileNum, &d.writePos); err != nil {
		return err
	}
	if len(lines) >= 4 {
		// optional best-effort field
		fmt.Sscanf(lines[3], "%d", &d.lastBatchID)
	}
	d.depth = depth
	d.nextReadFileNum = d.readFileNum
	d.nextReadPos = d.readPos

	// if the metadata was not sync'd at the last shutdown of nsqd
	// then the actual file size might actually be larger than the writePos,
	// in which case the safest thing to do is skip to the next file for
	// writes, and let the reader salvage what it can from the messages in the
	// diskqueue beyond the metadata's likely also stale readPos
	fileName = d.fileName(d.writeFileNum)
	fileInfo, err := os.Stat(fileName)
	if err != nil {
		return err
	}
	fileSize := fileInfo.Size()
	if d.writePos < fileSize {
		d.logf(WARN,
			"DISKQUEUE(%s) %s metadata writePos %d < file size of %d, skipping to new file",
			d.name, fileName, d.writePos, fileSize)
		d.writeFileNum += 1
		d.writePos = 0
		if d.writeFile != nil {
			d.writeFile.Close()
			d.writeFile = nil
		}
	}

	return nil
}

// persistMetaData atomically writes state to the filesystem
func (d *diskQueue) persistMetaData() error {
	var f *os.File
	var err error

	fileName := d.metaDataFileName()
	tmpFileName := fmt.Sprintf("%s.%d.tmp", fileName, rand.Int())

	// write to tmp file
	f, err = os.OpenFile(tmpFileName, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return err
	}

	_, err = fmt.Fprintf(f, "%d\n%d,%d\n%d,%d\n%d\n",
		d.depth,
		d.readFileNum, d.readPos,
		d.writeFileNum, d.writePos,
		d.lastBatchID)
	if err != nil {
		f.Close()
		return err
	}
	f.Sync()
	f.Close()

	// atomically rename
	return os.Rename(tmpFileName, fileName)
}

func (d *diskQueue) metaDataFileName() string {
	return fmt.Sprintf(path.Join(d.dataPath, "%s.diskqueue.meta.dat"), d.name)
}

func (d *diskQueue) fileName(fileNum int64) string {
	return fmt.Sprintf(path.Join(d.dataPath, "%s.diskqueue.%06d.dat"), d.name, fileNum)
}

// batchStartInfo records where an open (not yet committed) batch begins.
type batchStartInfo struct {
	id           uint64
	count        int32
	seen         int32
	fileNum      int64
	pos          int64
	beginPayload []byte
}

// dataFrameRef points at a deliverable message frame. remaining is the
// number of member frames from this one up to and including its batch's
// commit tail; zero means the frame is a standalone (non-batch) message.
type dataFrameRef struct {
	fileNum   int64
	pos       int64
	remaining int32
}

// recoverBatches scans every data file after a restart. Half-written
// batches (a begin frame with no valid commit tail) are discarded as a
// whole, while later, legal records remain readable. It also reconciles
// the write tail, read cursor, depth and the committed batch ID registry.
func (d *diskQueue) recoverBatches() error {
	prefix := d.name + ".diskqueue."
	matches, err := filepath.Glob(filepath.Join(d.dataPath, prefix+"*.dat"))
	if err != nil {
		return err
	}

	var fileNums []int64
	for _, name := range matches {
		base := filepath.Base(name)
		if !strings.HasPrefix(base, prefix) || !strings.HasSuffix(base, ".dat") {
			continue
		}
		numStr := base[len(prefix) : len(base)-len(".dat")]
		fn, parseErr := strconv.ParseInt(numStr, 10, 64)
		if parseErr != nil {
			continue
		}
		fileNums = append(fileNums, fn)
	}
	sort.Slice(fileNums, func(i, j int) bool { return fileNums[i] < fileNums[j] })

	committedIDs := make(map[uint64]struct{})
	var lastID uint64
	var frames []dataFrameRef
	var open *batchStartInfo
	// orphanStart is the index into frames where a run of member frames
	// without a surviving begin frame starts (the begin's file was already
	// removed once those members were read). -1 means no active run.
	orphanStart := -1
	var lastFileNum int64

	// dropOpen discards an incomplete batch: truncate the file holding its
	// begin frame back to the batch start offset and remove every later
	// file (which can only contain frames of the same batch).
	dropOpen := func(curFile int64) {
		if open == nil {
			return
		}
		cut := 0
		for _, fr := range frames {
			if fr.fileNum < open.fileNum ||
				(fr.fileNum == open.fileNum && fr.pos < open.pos) {
				frames[cut] = fr
				cut++
			}
		}
		frames = frames[:cut]

		if f, openErr := os.OpenFile(d.fileName(open.fileNum), os.O_RDWR, 0600); openErr == nil {
			if truncErr := f.Truncate(open.pos); truncErr != nil {
				d.logf(ERROR, "DISKQUEUE(%s) failed to truncate incomplete batch - %s", d.name, truncErr)
			}
			f.Close()
		}
		for fn := open.fileNum + 1; fn <= curFile; fn++ {
			if removeErr := os.Remove(d.fileName(fn)); removeErr != nil && !os.IsNotExist(removeErr) {
				d.logf(ERROR, "DISKQUEUE(%s) failed to remove incomplete batch file - %s", d.name, removeErr)
			}
		}
		d.logf(WARN, "DISKQUEUE(%s): discarding incomplete batch %d at %s:%d",
			d.name, open.id, d.fileName(open.fileNum), open.pos)
		open = nil
	}

	// quarantine renames a file whose corruption is not part of an open
	// batch, mirroring the runtime handleReadError behavior.
	quarantine := func(fn int64) {
		cut := 0
		for _, fr := range frames {
			if fr.fileNum != fn {
				frames[cut] = fr
				cut++
			}
		}
		frames = frames[:cut]
		// frame indices shifted and any orphan run intersecting this file
		// can no longer be attributed to a commit tail
		orphanStart = -1

		badFn := d.fileName(fn) + ".bad"
		d.logf(WARN, "DISKQUEUE(%s): quarantining corrupt file as %s", d.name, badFn)
		if renameErr := os.Rename(d.fileName(fn), badFn); renameErr != nil && !os.IsNotExist(renameErr) {
			d.logf(ERROR, "DISKQUEUE(%s) failed to rename corrupt file - %s", d.name, renameErr)
		}
	}

	for _, fn := range fileNums {
		lastFileNum = fn
		f, openErr := os.Open(d.fileName(fn))
		if openErr != nil {
			if os.IsNotExist(openErr) {
				continue
			}
			return openErr
		}

		pos := int64(0)
		failed := false

		for {
			var frameSize int32
			if readErr := binary.Read(f, binary.BigEndian, &frameSize); readErr != nil {
				if readErr != io.EOF {
					failed = true
				}
				break
			}

			switch {
			case frameSize == batchBeginMagic:
				payload := make([]byte, 12)
				if _, readErr := io.ReadFull(f, payload); readErr != nil {
					failed = true
					break
				}
				id := binary.BigEndian.Uint64(payload[:8])
				count := int32(binary.BigEndian.Uint32(payload[8:]))
				if count <= 0 {
					failed = true
					break
				}
				// a begin while another batch is open means the previous
				// batch never committed and must be discarded first
				if open != nil {
					dropOpen(fn)
				}
				open = &batchStartInfo{
					id:           id,
					count:        count,
					fileNum:      fn,
					pos:          pos,
					beginPayload: payload,
				}
				pos += batchBeginFrameLen

			case frameSize == batchCommitMagic:
				payload := make([]byte, 16)
				if _, readErr := io.ReadFull(f, payload); readErr != nil {
					failed = true
					break
				}
				id := binary.BigEndian.Uint64(payload[:8])
				count := int32(binary.BigEndian.Uint32(payload[8:12]))
				checksum := binary.BigEndian.Uint32(payload[12:])

				if open != nil {
					// normal case: the begin frame survived on disk
					if id != open.id || count != open.count ||
						checksum != crc32.ChecksumIEEE(open.beginPayload) {
						failed = true
						break
					}
				} else if orphanStart >= 0 {
					// the begin frame (and some early files) were removed
					// after their members were acknowledged; the surviving
					// orphan members still belong to this committed batch.
					// Validate that their count does not exceed the batch.
					if int(count) < len(frames)-orphanStart {
						failed = true
						break
					}
				} else {
					failed = true
					break
				}

				committedIDs[id] = struct{}{}
				if id > lastID {
					lastID = id
				}
				// backfill remaining counts. With a live open batch use its
				// declared count; otherwise attribute every orphan frame.
				start := orphanStart
				if open != nil {
					start = len(frames) - int(open.count)
				}
				if start >= 0 {
					for j := start; j < len(frames); j++ {
						frames[j].remaining = int32(len(frames) - j)
					}
				}
				open = nil
				orphanStart = -1
				pos += batchCommitFrameLen

			case frameSize >= d.minMsgSize && frameSize <= d.maxMsgSize:
				payload := make([]byte, frameSize)
				if _, readErr := io.ReadFull(f, payload); readErr != nil {
					failed = true
					break
				}
				if open != nil {
					open.seen++
					if open.seen > open.count {
						failed = true
						break
					}
				} else if orphanStart < 0 {
					// begin frame already gone: start tracking orphan members
					orphanStart = len(frames)
				}
				frames = append(frames, dataFrameRef{fileNum: fn, pos: pos})
				pos += int64(4 + frameSize)

			default:
				failed = true
			}

			if failed {
				break
			}
		}

		f.Close()

		if failed {
			if open != nil {
				dropOpen(fn)
			} else {
				quarantine(fn)
			}
		}
	}

	// a clean EOF with a batch still open is the classic "process died
	// mid-batch" case: the half batch sits at the physical write tail
	if open != nil {
		dropOpen(lastFileNum)
	}

	// re-discover the surviving files; the write tail is the end of the
	// highest one
	survivors, err := filepath.Glob(filepath.Join(d.dataPath, prefix+"*.dat"))
	if err != nil {
		return err
	}
	tailFileNum := d.writeFileNum
	tailPos := int64(0)
	best := int64(-1)
	for _, name := range survivors {
		base := filepath.Base(name)
		if !strings.HasPrefix(base, prefix) || !strings.HasSuffix(base, ".dat") {
			continue
		}
		fn, parseErr := strconv.ParseInt(base[len(prefix):len(base)-len(".dat")], 10, 64)
		if parseErr != nil || fn <= best {
			continue
		}
		info, statErr := os.Stat(name)
		if statErr != nil {
			continue
		}
		best = fn
		tailFileNum = fn
		tailPos = info.Size()
	}
	if best < 0 {
		// nothing survived: keep numbering from metadata (or 0)
		tailFileNum = d.writeFileNum
		tailPos = 0
	}

	// snap the read cursor to the first surviving frame at or after it;
	// batch marker frames and dropped files are transparently skipped
	idx := 0
	for idx < len(frames) {
		fr := frames[idx]
		if fr.fileNum > d.readFileNum ||
			(fr.fileNum == d.readFileNum && fr.pos >= d.readPos) {
			break
		}
		idx++
	}
	if idx == len(frames) {
		d.readFileNum = tailFileNum
		d.readPos = tailPos
		d.resumeRemaining = 0
	} else {
		d.readFileNum = frames[idx].fileNum
		d.readPos = frames[idx].pos
		// non-zero when the restored cursor lands inside a committed
		// batch: prefetch must resume from this member to its commit tail
		d.resumeRemaining = frames[idx].remaining
	}
	d.nextReadFileNum = d.readFileNum
	d.nextReadPos = d.readPos

	d.writeFileNum = tailFileNum
	d.writePos = tailPos
	d.committedFileNum = tailFileNum
	d.committedPos = tailPos
	d.depth = int64(len(frames) - idx)
	d.committedBatchIDs = committedIDs
	if lastID > d.lastBatchID {
		d.lastBatchID = lastID
	}

	d.needSync = true
	return nil
}

func (d *diskQueue) checkTailCorruption(depth int64) {
	if d.readFileNum < d.committedFileNum || d.readPos < d.committedPos {
		return
	}

	// we've reached the end of the diskqueue
	// if depth isn't 0 something went wrong
	if depth != 0 {
		if depth < 0 {
			d.logf(ERROR,
				"DISKQUEUE(%s) negative depth at tail (%d), metadata corruption, resetting 0...",
				d.name, depth)
		} else if depth > 0 {
			d.logf(ERROR,
				"DISKQUEUE(%s) positive depth at tail (%d), data loss, resetting 0...",
				d.name, depth)
		}
		// force set depth 0
		d.depth = 0
		d.needSync = true
	}

	if d.readFileNum != d.committedFileNum || d.readPos != d.committedPos {
		if d.readFileNum > d.committedFileNum {
			d.logf(ERROR,
				"DISKQUEUE(%s) readFileNum > committedFileNum (%d > %d), corruption, skipping to next writeFileNum and resetting 0...",
				d.name, d.readFileNum, d.committedFileNum)
		}

		if d.readPos > d.committedPos {
			d.logf(ERROR,
				"DISKQUEUE(%s) readPos > committedPos (%d > %d), corruption, skipping to next writeFileNum and resetting 0...",
				d.name, d.readPos, d.committedPos)
		}

		d.skipToNextRWFile()
		d.needSync = true
	}
}

func (d *diskQueue) moveForward(msg *readyMsg) {
	oldReadFileNum := d.readFileNum
	d.readFileNum = msg.nextFileNum
	d.readPos = msg.nextPos
	d.depth -= 1

	// see if we need to clean up the old file
	if oldReadFileNum != msg.nextFileNum {
		// sync every time we start reading from a new file
		d.needSync = true

		fn := d.fileName(oldReadFileNum)
		err := os.Remove(fn)
		if err != nil {
			d.logf(ERROR, "DISKQUEUE(%s) failed to Remove(%s) - %s", d.name, fn, err)
		}
	}

	// persist progress as soon as a whole batch has been acknowledged,
	// so a restart cannot redeliver confirmed members
	if msg.endOfBatch {
		d.needSync = true
	}

	d.checkTailCorruption(d.depth)
}

func (d *diskQueue) handleReadError() {
	// jump to the next read file and rename the current (bad) file
	if d.readFileNum == d.writeFileNum {
		// if you can't properly read from the current write file it's safe to
		// assume that something is fucked and we should skip the current file too
		if d.writeFile != nil {
			d.writeFile.Close()
			d.writeFile = nil
		}
		d.writeFileNum++
		d.writePos = 0
		d.committedFileNum = d.writeFileNum
		d.committedPos = 0
	}

	badFn := d.fileName(d.readFileNum)
	badRenameFn := badFn + ".bad"

	d.logf(WARN,
		"DISKQUEUE(%s) jump to next file and saving bad file as %s",
		d.name, badRenameFn)

	err := os.Rename(badFn, badRenameFn)
	if err != nil {
		d.logf(ERROR,
			"DISKQUEUE(%s) failed to rename bad diskqueue file %s to %s",
			d.name, badFn, badRenameFn)
	}

	d.readFileNum++
	d.readPos = 0
	d.nextReadFileNum = d.readFileNum
	d.nextReadPos = 0
	d.readAhead = nil

	// significant state change, schedule a sync on the next iteration
	d.needSync = true

	d.checkTailCorruption(d.depth)
}

// handleIncompleteBatch is the runtime counterpart of the startup
// recoverBatches scan: an uncommitted batch can only reach a reader via
// external tampering or a rolled-back write, and the same scan/truncate
// machinery safely restores the committed tail.
func (d *diskQueue) handleIncompleteBatch() {
	d.logf(WARN, "DISKQUEUE(%s): salvaging incomplete batch at %s:%d",
		d.name, d.fileName(d.readFileNum), d.readPos)

	d.closeReadFile()
	if d.writeFile != nil {
		d.writeFile.Close()
		d.writeFile = nil
	}

	if err := d.recoverBatches(); err != nil {
		d.logf(ERROR, "DISKQUEUE(%s) failed to salvage incomplete batch - %s", d.name, err)
	}
	d.needSync = true
}

// ioLoop provides the backend for exposing a go channel (via ReadChan())
// in support of multiple concurrent queue consumers
//
// it works by looping and branching based on whether or not the queue has data
// to read and blocking until data is either read or written over the appropriate
// go channels
//
// conveniently this also means that we're asynchronously reading from the filesystem
func (d *diskQueue) ioLoop() {
	var ready *readyMsg
	var err error
	var count int64
	var r chan []byte
	var p chan []byte

	syncTicker := time.NewTicker(d.syncTimeout)

	for {
		if len(d.readAhead) == 0 {
			ready = nil
		}

		// dont sync all the time :)
		if count == d.syncEvery {
			d.needSync = true
		}

		if d.needSync {
			err = d.sync()
			if err != nil {
				d.logf(ERROR, "DISKQUEUE(%s) failed to sync - %s", d.name, err)
			}
			count = 0
		}

		if ready == nil &&
			(d.readFileNum < d.committedFileNum ||
				(d.readFileNum == d.committedFileNum && d.readPos < d.committedPos)) {
			var msgs []readyMsg
			msgs, err = d.prefetch()
			if err != nil {
				if err == errEOFAtFrameBoundary {
					d.advanceToNextFile()
					continue
				}
				d.logf(ERROR, "DISKQUEUE(%s) reading at %d of %s - %s",
					d.name, d.readPos, d.fileName(d.readFileNum), err)
				if err == errIncompleteBatch {
					d.handleIncompleteBatch()
				} else {
					d.handleReadError()
				}
				continue
			}
			d.readAhead = append(d.readAhead, msgs...)
			ready = &d.readAhead[0]
		}

		var readyData []byte
		if ready != nil {
			readyData = ready.data
			r = d.readChan
			p = d.peekChan
		} else {
			r = nil
			p = nil
		}

		select {
		// the Go channel spec dictates that nil channel operations (read or write)
		// in a select are skipped, we set r to d.readChan only when there is data to read
		case p <- readyData:
		case r <- readyData:
			count++
			d.moveForward(ready)
			d.readAhead = d.readAhead[1:]
			if len(d.readAhead) > 0 {
				ready = &d.readAhead[0]
			} else {
				ready = nil
			}
		case d.depthChan <- d.depth:
		case <-d.emptyChan:
			d.emptyResponseChan <- d.deleteAllFiles()
			d.readAhead = nil
			ready = nil
			count = 0
		case dataWrite := <-d.writeChan:
			count++
			d.writeResponseChan <- d.writeOne(dataWrite)
		case req := <-d.batchChan:
			count++
			d.writeResponseChan <- d.writeBatch(req.id, req.data)
		case <-syncTicker.C:
			if count == 0 {
				// avoid sync when there's no activity
				continue
			}
			d.needSync = true
		case <-d.exitChan:
			goto exit
		}
	}

exit:
	d.logf(INFO, "DISKQUEUE(%s): closing ... ioLoop", d.name)
	syncTicker.Stop()
	d.exitSyncChan <- 1
}
