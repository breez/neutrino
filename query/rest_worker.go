package query

import (
	"context"
	"fmt"

	"github.com/btcsuite/btcd/wire"
)

type restWorker struct {
	peer    *restPeer
	nextJob chan *queryJob
}

func NewRestWorker(peer *restPeer) Worker {
	return &restWorker{
		peer:    peer,
		nextJob: make(chan *queryJob, 1),
	}
}

func (w *restWorker) Run(results chan<- *jobResult, quit <-chan struct{}) {
	for {
		var job *queryJob
		select {
		case job = <-w.nextJob:
			log.Tracef("Rest worker '%v' picked up job with index %v",
				w.peer.Addr(), job.Index())
		case <-quit:
			return
		}

		select {
		// There is no point in queueing the request if the job already
		// is canceled, so we check this quickly.
		case <-job.cancelChan:
			log.Tracef("Rest worker '%v' found job with index %v "+
				"already canceled", w.peer.Addr(), job.Index())

		// We received a non-canceled query job, send it to the peer.
		default:
			log.Tracef("Rest worker '%v' queuing job %T with index %v",
				w.peer.Addr(), job.Req, job.Index())

			go w.handleJob(job, results, quit)
		}
	}
}

func (w *restWorker) handleJob(job *queryJob, results chan<- *jobResult, quit <-chan struct{}) {
	ctx, cancel := context.WithTimeout(context.Background(), job.timeout)
	defer cancel()

	// Ensure the request is cancelled when the job is cancelled.
	go func() {
		select {
		case <-ctx.Done():
		case <-job.cancelChan:
			cancel()
		}
	}()

	msgs, err := w.peer.Query(ctx, job.RestReq)
	if err != nil {
		if ctx.Err() != nil {
			err = ErrJobCanceled
		}
	} else {
		err = w.handleMsgs(msgs, job)
	}

	select {
	case results <- &jobResult{
		job:  job,
		peer: w.peer,
		err:  err,
	}:
	case <-quit:
		return
	}
}

func (w *restWorker) handleMsgs(msgs []wire.Message, job *queryJob) error {
	for _, msg := range msgs {
		progress := job.HandleResp(job.Req, msg, w.peer.Addr())
		if progress.Finished {
			return nil
		}
	}

	return fmt.Errorf("got entire filter, but job was not finished")
}

func (w *restWorker) NewJob() chan<- *queryJob {
	return w.nextJob
}

func (w *restWorker) SupportsJob(req *Request) bool {
	// If this is not a rest-able request, this can't handle it.
	if req.RestReq == "" {
		return false
	}

	// Tell the manager we can't handle if the rest peer is in backoff right now.
	return !w.peer.Backoff()
}
