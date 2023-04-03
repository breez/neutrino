package neutrino

import (
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"time"

	"github.com/btcsuite/btcd/wire"
)

func (s *ChainService) queryRestPeers(query *cfiltersQuery) {
	quit := make(chan struct{})
	client := &http.Client{}
	validPeers := make([]int, 0, len(s.restPeers))
	for i, p := range s.restPeers {
		if p.failures == 0 || time.Since(p.lastFailure) > 10*time.Second {
			validPeers = append(validPeers, i)
		}
	}
	if len(validPeers) == 0 {
		log.Errorf("queryRestPeers - No valid rest peer")
		return
	}
	// #nosec G404 -- No need to have true randomness when selecting restpeer.
	restPeerIndex := validPeers[rand.Intn(len(validPeers))]
	URL := fmt.Sprintf("%v/rest/blockfilter/basic/%v.bin?count=%v", s.restPeers[restPeerIndex].URL, query.stopHash.String(), query.stopHeight-query.startHeight+1)
	log.Infof("getting %v filters from height %v to height %v, using URL: %v", query.stopHeight-query.startHeight+1, query.startHeight, query.stopHeight, URL)
	res, err := client.Get(URL)
	if err != nil {
		s.restPeers[restPeerIndex].failures++
		s.restPeers[restPeerIndex].lastFailure = time.Now()
		log.Errorf("queryRestPeers - Get (%v) error: %v", URL, err)
		return
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		s.restPeers[restPeerIndex].failures++
		s.restPeers[restPeerIndex].lastFailure = time.Now()
		log.Errorf("queryRestPeers - Get (%v) status != OK: %v", URL, res.Status)
		_, err = io.Copy(io.Discard, res.Body)
		if err != nil {
			log.Errorf("error in io.Copy: %v", err)
		}
		return
	}
	s.restPeers[restPeerIndex].failures = 0

	// Creating message and deserialising the results.
	log.Infof("Calling handleCFilterRestponse for each received filter from URL: %v", URL)
	count := 0
	for {
		count++
		filter := &wire.MsgCFilter{}
		err = filter.BtcDecode(res.Body, 0, wire.BaseEncoding)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Errorf("error deserialising object: %v", err)
			return
		}
		s.handleCFiltersResponse(query, filter, quit)
	}
	log.Infof("Called handleCFilterRestponse for %v filter received from URL: %v", count, URL)
}
