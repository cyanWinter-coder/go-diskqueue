# go-diskqueue

[![Build Status](https://github.com/nsqio/go-diskqueue/workflows/tests/badge.svg)](https://github.com/nsqio/go-diskqueue/actions) [![Go Reference](https://pkg.go.dev/badge/github.com/nsqio/go-diskqueue.svg)](https://pkg.go.dev/github.com/nsqio/go-diskqueue) [![GitHub release](https://img.shields.io/github/release/nsqio/go-diskqueue.svg)](https://github.com/nsqio/go-diskqueue/releases/latest)

A Go package providing a filesystem-backed FIFO queue

Pulled out of https://github.com/nsqio/nsq

## Atomic batches

In addition to the single-message `Put` / `ReadChan` API, a queue created by
`New` also implements `BatchInterface`, which exposes
`PutBatch(id uint64, data [][]byte) error`.

A batch is a group of related messages written as one recoverable, atomic
unit:

- Every batch carries a stable, producer-chosen `id` and a member count. The
  member frames are followed by a commit tail record; the batch becomes
  visible to readers only after all of those frames are `fsync`'d together
  with the metadata.
- If a process dies after writing only part of a batch, the next `New`
  detects the missing commit tail while scanning the data files and discards
  the whole half batch. Later, fully committed records remain readable.
- Committed batches are delivered in their original order. Restarting after
  reading part of a batch never redelivers the members that were already
  acknowledged.
- File rotation and depth are computed over whole batches; a commit tail is
  never split from its batch across a rotation boundary.
- A retried `PutBatch` whose `id` was already committed fails with
  `ErrDuplicateBatch`; a commit whose sync fails rolls the partial batch back
  and returns `ErrBatchCommit`, leaving every previously committed message
  intact. Operations on a closed queue return `ErrExiting`.

The single-message `Put` and `ReadChan` behavior is unchanged and can be used
alongside batches.

