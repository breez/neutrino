package neutrino

import (
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"time"

	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
)

func (s *ChainService) queryRestPeers(
	blockHash chainhash.Hash, query *cfiltersQuery) {
	quit := make(chan struct{})
	client := &http.Client{Timeout: QueryTimeout}
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
	restPeerIndex := validPeers[rand.Intn(len(validPeers))]
	URL := fmt.Sprintf("%v/rest/blockfilter/basic/%v.bin?count=%v", s.restPeers[restPeerIndex].URL, query.stopHash.String(), query.stopHeight-query.startHeight+1)
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
		io.Copy(ioutil.Discard, res.Body)
		return
	}
	s.restPeers[restPeerIndex].failures = 0

	// Creating message and deserialising the results.
	for {
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
}
