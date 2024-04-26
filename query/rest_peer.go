package query

import (
	"context"
	"io"
	"net/http"
	"time"

	"github.com/btcsuite/btcd/wire"
)

const BackoffPeriod time.Duration = time.Second * 10

type restPeer struct {
	url     string
	lastErr time.Time
}

func NewRestPeer(url string) *restPeer {
	return &restPeer{
		url:     url,
		lastErr: time.Time{},
	}
}

func (p *restPeer) Addr() string {
	return p.url
}

func (p *restPeer) Backoff() bool {
	return time.Since(p.lastErr) < BackoffPeriod
}

func (p *restPeer) Query(ctx context.Context, suffix string) ([]wire.Message, error) {
	client := &http.Client{}
	url := p.url + suffix

	// Note this request is cancellable
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	// Perform the request
	res, err := client.Do(req)
	if err != nil {
		log.Errorf("Rest peer query '%s' returned error '%v'", url, err)
		p.lastErr = time.Now()
		return nil, err
	}

	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		log.Errorf("Rest peer query '%s' status (%v) != OK", url, res.Status)
		p.lastErr = time.Now()
		_, err = io.Copy(io.Discard, res.Body)
		if err != nil {
			log.Errorf("error in io.Copy: %v", err)
		}
		return nil, err
	}

	var msgs []wire.Message
	for {
		filter := &wire.MsgCFilter{}
		err = filter.BtcDecode(res.Body, 0, wire.BaseEncoding)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Errorf("error deserializing expected CFilter: %v", err)
			return nil, err
		}
		msgs = append(msgs, filter)
	}

	log.Debugf("Received %d CFilters from url: %s", len(msgs), url)
	return msgs, nil
}
