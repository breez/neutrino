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
	blockHash chainhash.Hash,
	query *cfiltersQuery,
	numWorkers int,
	startBlock int64,
	endBlock int64) {
	quit := make(chan struct{})
	client := &http.Client{Timeout: QueryTimeout}
	blockHeaders, err := s.BlockHeaders.FetchHeaderByHeight(uint32(endBlock))
	if err != nil {
		log.Errorf("unable to get header for start "+
			"block=%v: %v", blockHash, err)
		return
	}
	hash := blockHeaders.BlockHash()
	validPeers := make([]int, 0, len(s.restPeers))
	for i, p := range s.restPeers {
		if p.failures == 0 || time.Since(p.lastFailure) > 10*time.Second {
			validPeers = append(validPeers, i)
		}
	}
	restPeerIndex := validPeers[rand.Intn(len(validPeers))]
	URL := fmt.Sprintf("%v/rest/blockfilter/basic/%v.bin?count=%v", s.restPeers[restPeerIndex].URL, hash.String(), endBlock-startBlock+1)
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
