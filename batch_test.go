package diskqueue

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"
)

func newTestBatchQueue(t *testing.T, maxBytes int64) (BatchInterface, string) {
	l := NewTestLogger(t)
	dqName := "test_batch_queue" + strconv.Itoa(int(time.Now().UnixNano()))
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-batch-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	dq := New(dqName, tmpDir, maxBytes, 0, 1<<20, 2500, 2*time.Second, l)
	return dq.(BatchInterface), tmpDir
}

func batchMsgs(prefix byte, n int) [][]byte {
	out := make([][]byte, n)
	for i := range out {
		out[i] = []byte{prefix, byte(i)}
	}
	return out
}

func drain(t *testing.T, dq Interface, want [][]byte) {
	t.Helper()
	for i, msg := range want {
		got := <-dq.ReadChan()
		Equal(t, msg, got)
		_ = i
	}
}

// 1. basic atomic batch: stable ID + count, invisible until commit,
// delivered in order, depth only advances by the full batch on commit.
func TestBatchPutCommit(t *testing.T) {
	dq, tmpDir := newTestBatchQueue(t, 1024*1024)
	defer os.RemoveAll(tmpDir)
	defer dq.Close()

	Equal(t, int64(0), dq.Depth())

	msgs := batchMsgs('a', 5)
	Nil(t, dq.PutBatch(42, msgs))
	Equal(t, int64(5), dq.Depth())

	for i, msg := range msgs {
		got := <-dq.ReadChan()
		Equal(t, msg, got)
		Equal(t, int64(4-i), dq.Depth())
	}
	Equal(t, int64(0), dq.Depth())

	// single Put still works alongside batches, preserving FIFO order
	Nil(t, dq.Put([]byte("solo")))
	Nil(t, dq.PutBatch(43, batchMsgs('b', 2)))
	Equal(t, []byte("solo"), <-dq.ReadChan())
	drain(t, dq, batchMsgs('b', 2))
}

// 1b. a batch must not be readable until the commit tail record is durable.
// While a batch write is in flight (synchronously: PutBatch only returns
// after commit), so we instead verify the on-disk framing: begin frame,
// member frames and commit tail all exist after a successful PutBatch.
func TestBatchOnDiskFraming(t *testing.T) {
	dq, tmpDir := newTestBatchQueue(t, 1024*1024)
	defer os.RemoveAll(tmpDir)
	defer dq.Close()

	msgs := batchMsgs('a', 3)
	Nil(t, dq.PutBatch(7, msgs))
	dq.Close()

	files, _ := filepath.Glob(filepath.Join(tmpDir, "*.diskqueue.[0-9]*.dat"))
	if len(files) != 1 {
		t.Fatalf("expected 1 data file, got %d", len(files))
	}
	raw, err := ioutil.ReadFile(files[0])
	Nil(t, err)

	expectedSize := batchBeginFrameLen + 3*(4+2) + batchCommitFrameLen
	Equal(t, expectedSize, len(raw))
}

// 2. restart with a half-written batch (begin + members, no commit tail):
// the whole half batch is discarded but later, legal records stay readable.
func TestBatchRecoveryDiscardsHalfBatch(t *testing.T) {
	dq, tmpDir := newTestBatchQueue(t, 1024*1024)
	defer os.RemoveAll(tmpDir)

	goodA := batchMsgs('a', 3)
	Nil(t, dq.PutBatch(1, goodA))
	Nil(t, dq.Put([]byte("solo")))
	Nil(t, dq.Close())

	// append a torn batch directly to the surviving data file
	inner := dq.(*diskQueue)
	fn := inner.fileName(inner.writeFileNum)
	f, err := os.OpenFile(fn, os.O_RDWR|os.O_APPEND, 0600)
	Nil(t, err)

	begin := batchBeginPayload(99, 2)
	buf := make([]byte, 0, 64)
	buf = appendFrame(buf, batchBeginMagic, begin)
	buf = appendDataFrame(buf, []byte{'x', 0})
	buf = appendDataFrame(buf, []byte{'x', 1})
	// deliberately no commit frame
	_, err = f.Write(buf)
	Nil(t, err)
	Nil(t, f.Sync())
	Nil(t, f.Close())

	// a valid batch written *after* the torn batch (simulating a later run)
	l := NewTestLogger(t)
	dq2 := New(inner.name, tmpDir, 1024*1024, 0, 1<<20, 2500, 2*time.Second, l)
	defer dq2.Close()

	// first the pre-crash survivors: goodA then solo; the torn batch is gone
	want := append(batchMsgs('a', 3), []byte("solo"))
	drain(t, dq2, want)
	Equal(t, int64(0), dq2.Depth())

	// a later, legal batch stays fully readable after recovery
	goodB := batchMsgs('b', 2)
	Nil(t, dq2.(BatchInterface).PutBatch(100, goodB))
	drain(t, dq2, goodB)
	Equal(t, int64(0), dq2.Depth())
}

// 2b. torn batch spanning a file rotation: begin in one file, members in the
// next, crash before commit. Both pieces must be discarded wholesale.
func TestBatchRecoveryHalfBatchAcrossFiles(t *testing.T) {
	l := NewTestLogger(t)
	dqName := "test_batch_span" + strconv.Itoa(int(time.Now().UnixNano()))
	tmpDir, err := ioutil.TempDir("", "nsq-batch-span")
	Nil(t, err)
	defer os.RemoveAll(tmpDir)

	dqIface := New(dqName, tmpDir, 1024, 0, 1<<20, 2500, 2*time.Second, l)
	dq := dqIface.(*diskQueue)

	// one committed batch establishing a valid prefix in file 0
	Nil(t, dqIface.(BatchInterface).PutBatch(1, [][]byte{{'a', 0}}))
	Nil(t, dq.Close())

	// hand-craft a torn batch across two brand-new files: begin in N+1,
	// its member in N+2, with no commit tail anywhere
	nextFile := dq.writeFileNum + 1
	writeRaw := func(fileNum int64, b []byte) {
		fn := dq.fileName(fileNum)
		f, openErr := os.OpenFile(fn, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
		Nil(t, openErr)
		_, openErr = f.Write(b)
		Nil(t, openErr)
		Nil(t, f.Sync())
		Nil(t, f.Close())
	}
	begin := batchBeginPayload(50, 1)
	writeRaw(nextFile, appendFrame(nil, batchBeginMagic, begin))
	writeRaw(nextFile+1, appendDataFrame(nil, []byte{'z', 0}))

	dq2Iface := New(dqName, tmpDir, 1024, 0, 1<<20, 2500, 2*time.Second, l)
	defer dq2Iface.Close()
	dq2 := dq2Iface.(BatchInterface)

	// the torn batch consumed no depth; only the committed batch remains
	Equal(t, int64(1), dq2.Depth())
	Equal(t, []byte{'a', 0}, <-dq2.ReadChan())
	Equal(t, int64(0), dq2.Depth())

	// the file holding torn members is removed wholesale; the begin file is
	// truncated back to empty (its batch-start offset was 0)
	assertFileNotExist(t, dq.fileName(nextFile+1))
	if info, statErr := os.Stat(dq.fileName(nextFile)); statErr == nil && info.Size() != 0 {
		t.Fatalf("torn begin file should be truncated to empty, size=%d", info.Size())
	}

	// the queue still accepts new batches and they are fully readable
	Nil(t, dq2.PutBatch(51, [][]byte{{'c', 0}}))
	Equal(t, []byte{'c', 0}, <-dq2.ReadChan())
}

// 3. committed batches deliver in order; restarting after consuming part of a
// batch never redelivers already-acknowledged members.
func TestBatchRestartNoRedelivery(t *testing.T) {
	dq, tmpDir := newTestBatchQueue(t, 1024*1024)
	defer os.RemoveAll(tmpDir)

	Nil(t, dq.PutBatch(1, batchMsgs('a', 4)))
	Nil(t, dq.PutBatch(2, batchMsgs('b', 4)))

	// consume 2 members of batch 1 and 0 of batch 2
	Equal(t, []byte{'a', 0}, <-dq.ReadChan())
	Equal(t, []byte{'a', 1}, <-dq.ReadChan())
	Nil(t, dq.Close())

	innerName := dq.(*diskQueue).name
	l := NewTestLogger(t)
	dq2 := New(innerName, tmpDir, 1024*1024, 0, 1<<20, 2500, 2*time.Second, l)
	defer dq2.Close()

	Equal(t, int64(6), dq2.Depth())
	want := [][]byte{{'a', 2}, {'a', 3}}
	want = append(want, batchMsgs('b', 4)...)
	drain(t, dq2, want)
	Equal(t, int64(0), dq2.Depth())
}

// 3b. a batch spanning multiple files, read halfway then restarted, never
// redelivers confirmed members and resumes exactly at the next member.
func TestBatchRestartMidBatchAcrossFiles(t *testing.T) {
	l := NewTestLogger(t)
	dqName := "test_batch_resume_span" + strconv.Itoa(int(time.Now().UnixNano()))
	tmpDir, err := ioutil.TempDir("", "nsq-batch-resume-span")
	Nil(t, err)
	defer os.RemoveAll(tmpDir)

	// small files force a single 12-member batch across several files
	dqIface := New(dqName, tmpDir, 40, 0, 1<<20, 2500, 2*time.Second, l)
	dq := dqIface.(*diskQueue)
	msgs := batchMsgs('m', 12)
	Nil(t, dqIface.(BatchInterface).PutBatch(1, msgs))
	if dq.writeFileNum == 0 {
		t.Fatalf("test setup: expected the batch to span files")
	}

	// acknowledge the first 5 members (likely crossing a file boundary)
	for i := 0; i < 5; i++ {
		Equal(t, msgs[i], <-dqIface.ReadChan())
	}
	Equal(t, int64(7), dqIface.Depth())
	Nil(t, dqIface.Close())

	dq2Iface := New(dqName, tmpDir, 40, 0, 1<<20, 2500, 2*time.Second, l)
	defer dq2Iface.Close()

	// depth is reconciled to the 7 unacknowledged members, not 12 or 0
	Equal(t, int64(7), dq2Iface.Depth())
	for i := 5; i < 12; i++ {
		Equal(t, msgs[i], <-dq2Iface.ReadChan())
	}
	Equal(t, int64(0), dq2Iface.Depth())

	bad, _ := filepath.Glob(filepath.Join(tmpDir, "*.bad"))
	if len(bad) != 0 {
		t.Fatalf("unexpected bad files: %v", bad)
	}
}

// 4. rotation and depth limits are computed per whole batch: a batch larger
// than one file spans files with its commit tail, is fully readable in order
// and leaves no .bad files behind.
func TestBatchRotationKeepsBatchTogether(t *testing.T) {
	l := NewTestLogger(t)
	dqName := "test_batch_rotate" + strconv.Itoa(int(time.Now().UnixNano()))
	tmpDir, err := ioutil.TempDir("", "nsq-batch-rotate")
	Nil(t, err)
	defer os.RemoveAll(tmpDir)

	// each frame is 14 bytes; maxBytesPerFile 40 means ~2 frames per file
	dq := New(dqName, tmpDir, 40, 0, 1<<20, 2500, 2*time.Second, l)
	defer dq.Close()

	msgs := batchMsgs('m', 12)
	Nil(t, dq.(BatchInterface).PutBatch(1, msgs))

	// more than one data file must have been used
	inner := dq.(*diskQueue)
	if inner.writeFileNum == 0 {
		t.Fatalf("expected batch to span multiple files")
	}

	drain(t, dq, msgs)
	Equal(t, int64(0), dq.Depth())

	bad, _ := filepath.Glob(filepath.Join(tmpDir, "*.bad"))
	if len(bad) != 0 {
		t.Fatalf("unexpected bad files: %v", bad)
	}

	// a second batch after rotation still commits and reads cleanly
	msgs2 := batchMsgs('n', 5)
	Nil(t, dq.(BatchInterface).PutBatch(2, msgs2))
	drain(t, dq, msgs2)
}

// 4b. the commit tail is never split from its batch: after any rotation, the
// first frame of a new file may only be a begin frame or member data, never a
// stranded commit frame. Verified by scanning every data file directly.
func TestBatchCommitTailNotStranded(t *testing.T) {
	l := NewTestLogger(t)
	dqName := "test_batch_tail" + strconv.Itoa(int(time.Now().UnixNano()))
	tmpDir, err := ioutil.TempDir("", "nsq-batch-tail")
	Nil(t, err)
	defer os.RemoveAll(tmpDir)

	dq := New(dqName, tmpDir, 30, 0, 1<<20, 2500, 2*time.Second, l)

	allMsgs := make([][]byte, 0, 60)
	for id := uint64(1); id <= 10; id++ {
		b := batchMsgs(byte('a'+id-1), 6)
		Nil(t, dq.(BatchInterface).PutBatch(id, b))
		allMsgs = append(allMsgs, b...)
	}
	Nil(t, dq.Close())

	// On-disk invariant, checked before any consumption (reading removes
	// fully consumed files): walking every frame in file order, each commit
	// tail closes the most recent open begin, and no file mixes the tail of
	// one batch with a different batch's content.
	files, _ := filepath.Glob(filepath.Join(tmpDir, "*.diskqueue.[0-9]*.dat"))
	sort.Strings(files)
	if len(files) < 2 {
		t.Fatalf("expected batches to span multiple files, got %d", len(files))
	}
	openBegins := 0
	for _, fn := range files {
		raw, readErr := ioutil.ReadFile(fn)
		Nil(t, readErr)
		for off := 0; off+4 <= len(raw); {
			magic := int32(beUint32(raw[off : off+4]))
			switch magic {
			case batchBeginMagic:
				openBegins++
				off += batchBeginFrameLen
			case batchCommitMagic:
				if openBegins == 0 {
					t.Fatalf("file %s has a commit frame with no matching batch", fn)
				}
				openBegins--
				off += batchCommitFrameLen
			default:
				size := beUint32(raw[off : off+4])
				off += 4 + int(size)
			}
		}
	}
	if openBegins != 0 {
		t.Fatalf("found %d batch begin frames without a matching commit", openBegins)
	}

	// reopen and read every committed batch back whole and in order across
	// all the rotations, with no quarantined files
	dq = New(dqName, tmpDir, 30, 0, 1<<20, 2500, 2*time.Second, l)
	drain(t, dq, allMsgs)
	Equal(t, int64(0), dq.Depth())
	Nil(t, dq.Close())

	bad, _ := filepath.Glob(filepath.Join(tmpDir, "*.bad"))
	if len(bad) != 0 {
		t.Fatalf("unexpected bad files: %v", bad)
	}
}

// 5a. duplicate batch IDs (retries) return an explicit sentinel error and do
// not mutate the queue.
func TestBatchDuplicateID(t *testing.T) {
	dq, tmpDir := newTestBatchQueue(t, 1024*1024)
	defer os.RemoveAll(tmpDir)
	defer dq.Close()

	Nil(t, dq.PutBatch(10, batchMsgs('a', 2)))

	err := dq.PutBatch(10, batchMsgs('b', 2))
	if !errors.Is(err, ErrDuplicateBatch) {
		t.Fatalf("expected ErrDuplicateBatch, got %v", err)
	}
	// an older ID is also rejected
	err = dq.PutBatch(9, batchMsgs('c', 2))
	if !errors.Is(err, ErrDuplicateBatch) {
		t.Fatalf("expected ErrDuplicateBatch, got %v", err)
	}
	Equal(t, int64(2), dq.Depth())
	drain(t, dq, batchMsgs('a', 2))

	// duplicate detection survives a restart (registry rebuilt from disk)
	name := dq.(*diskQueue).name
	Nil(t, dq.Close())
	l := NewTestLogger(t)
	dq2 := New(name, tmpDir, 1024*1024, 0, 1<<20, 2500, 2*time.Second, l)
	defer dq2.Close()

	err = dq2.(BatchInterface).PutBatch(10, batchMsgs('d', 1))
	if !errors.Is(err, ErrDuplicateBatch) {
		t.Fatalf("expected ErrDuplicateBatch after restart, got %v", err)
	}
}

// 5b. an fsync failure while committing rolls the partial batch back;
// previously committed data stays intact and readable, and the failed batch
// ID can be retried successfully.
func TestBatchSyncFailureRollback(t *testing.T) {
	dq, tmpDir := newTestBatchQueue(t, 1024*1024)
	defer os.RemoveAll(tmpDir)
	defer dq.Close()

	good := batchMsgs('g', 3)
	Nil(t, dq.PutBatch(1, good))
	Equal(t, int64(3), dq.Depth())

	inner := dq.(*diskQueue)
	var failOnce sync.Once
	inner.syncErrHook = func() error {
		var ret error
		failOnce.Do(func() {
			ret = errors.New("simulated fsync failure")
		})
		return ret
	}

	retry := batchMsgs('r', 4)
	err := dq.PutBatch(2, retry)
	if !errors.Is(err, ErrBatchCommit) {
		t.Fatalf("expected ErrBatchCommit, got %v", err)
	}

	// hook consumed; previously committed queue is untouched
	Equal(t, int64(3), inner.Depth())
	drain(t, dq, good)
	Equal(t, int64(0), dq.Depth())

	// the failed batch ID was never committed, so a retry succeeds
	Nil(t, dq.PutBatch(2, retry))
	drain(t, dq, retry)
	Equal(t, int64(0), dq.Depth())
}

// 5c. empty batches and post-close operations return explicit errors.
func TestBatchEmptyAndClosed(t *testing.T) {
	dq, tmpDir := newTestBatchQueue(t, 1024*1024)
	defer os.RemoveAll(tmpDir)

	if err := dq.PutBatch(1, nil); !errors.Is(err, ErrEmptyBatch) {
		t.Fatalf("expected ErrEmptyBatch, got %v", err)
	}

	Nil(t, dq.Close())

	if err := dq.Put([]byte{1}); !errors.Is(err, ErrExiting) {
		t.Fatalf("expected ErrExiting from Put, got %v", err)
	}
	if err := dq.PutBatch(2, [][]byte{{1}}); !errors.Is(err, ErrExiting) {
		t.Fatalf("expected ErrExiting from PutBatch, got %v", err)
	}
	if err := dq.Empty(); !errors.Is(err, ErrExiting) {
		t.Fatalf("expected ErrExiting from Empty, got %v", err)
	}
	// a second Close is a defined result, not a panic
	if err := dq.Close(); !errors.Is(err, ErrExiting) {
		t.Fatalf("expected ErrExiting from second Close, got %v", err)
	}
}

// 5d. concurrent Close while writers are active never panics and every
// writer either commits or receives ErrExiting.
func TestBatchConcurrentClose(t *testing.T) {
	dq, tmpDir := newTestBatchQueue(t, 1024*1024)
	defer os.RemoveAll(tmpDir)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				id := uint64(n*1000 + j + 1)
				if err := dq.PutBatch(id, [][]byte{{byte(n), byte(j)}}); err != nil {
					if !errors.Is(err, ErrExiting) && !errors.Is(err, ErrDuplicateBatch) {
						t.Errorf("unexpected error: %v", err)
						return
					}
					return
				}
			}
		}(i)
	}

	time.Sleep(20 * time.Millisecond)
	Nil(t, dq.Close())
	wg.Wait()
}

// test helpers for hand-crafting raw frames

func beUint32(b []byte) uint32 {
	return uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
}

func appendFrame(buf []byte, magic int32, payload []byte) []byte {
	hdr := make([]byte, 4)
	hdr[0] = byte(magic >> 24)
	hdr[1] = byte(magic >> 16)
	hdr[2] = byte(magic >> 8)
	hdr[3] = byte(magic)
	buf = append(buf, hdr...)
	return append(buf, payload...)
}

func appendDataFrame(buf []byte, data []byte) []byte {
	hdr := make([]byte, 4)
	n := uint32(len(data))
	hdr[0] = byte(n >> 24)
	hdr[1] = byte(n >> 16)
	hdr[2] = byte(n >> 8)
	hdr[3] = byte(n)
	buf = append(buf, hdr...)
	return append(buf, data...)
}
