package neutrino

import (
	"fmt"
	"io"
	"net/http"

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
	res, err := client.Get(fmt.Sprintf("%v/rest/blockfilter/basic/%v.bin?count=%v", s.restPeers[restHostIndex], hash.String(), endBlock-startBlock+1))
	// TODO(ubbabeck) add functionality to query another peer if avalible
	if err != nil {
		log.Errorf("error: %v", err)
		return
	}
	defer res.Body.Close()

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
