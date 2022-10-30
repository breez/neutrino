package neutrino

import (
	"bytes"
	"fmt"
	"io"
	"net/http"

	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
)

// Number of download workers that query rest peers in parallel
const numberOfRestWorkers = 10

func (s *ChainService) queryRestPeers(
	blockHash chainhash.Hash,
	query *cfiltersQuery,
	numWorkers int,
	startBlock int64,
	endBlock int64) {

	// Each block number represents a download job
	blockNumbers := make(chan int64, endBlock-startBlock+1)

	// Downloaded filters will be sent via this channel
	results := make(chan *wire.MsgCFilter, endBlock-startBlock+1)
	quit := make(chan struct{})
	// We'll need a http client in order to query the host
	client := &http.Client{Timeout: QueryTimeout}

	// We use 10 workers
	for i := 0; i < numberOfRestWorkers; i++ {

		// spin up a worker
		go func() {
			for blockNum := range blockNumbers {
				// Fetch blockheaders from persistent storage
				blockHeaders, err := s.BlockHeaders.FetchHeaderByHeight(uint32(blockNum))
				if err != nil {
					log.Errorf("unable to get header for start "+
						"block=%v: %v", blockHash, err)
					return
				}
				hash := blockHeaders.BlockHash()
				filter, err := s.getCFilterRest(hash, restHostIndex, client)
				if err != nil {
					log.Errorf("error: %w", err)
					return
				}
				results <- filter
			}
		}()
	}

	// Queue the block numbers for which we want to downoad filters
	for blockNum := startBlock; blockNum < endBlock+1; blockNum++ {
		blockNumbers <- blockNum
	}

	close(blockNumbers)

	// wait for the results in the results channel and invoke
	// the handler method on each filter.
	for j := startBlock; j < endBlock+1; j++ {
		filter := <-results
		s.handleCFiltersResponse(query, filter, quit)
	}
}

// getCfilterRest gets a cFilter from its peers. Given that, it supports the rest
// API.
func (s *ChainService) getCFilterRest(h chainhash.Hash, hostIndex int, c *http.Client) (*wire.MsgCFilter, error) {
	// Getting the basic blockfilter with the blockhash
	res, err := c.Get(fmt.Sprintf("%v/rest/blockfilter/basic/%v.bin", s.restPeers[hostIndex], h.String()))
	// TODO(ubbabeck) add functionality to query another peer if avalible
	if err != nil {
		return nil, fmt.Errorf("client: %w", err)
	}
	defer res.Body.Close()
	bodyBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, fmt.Errorf("http.Get(%v) error: %w", res, err)
	}

	// Creating message and deserialising the results.
	filter := &wire.MsgCFilter{}
	reader := bytes.NewBuffer(bodyBytes)
	err = filter.Deserialize(reader)
	if err != nil {
		return nil, fmt.Errorf("error deserialising object:%w", err)
	}
	log.Infof("Fetched CFilter for hash %v for host %v", h, s.restPeers[restHostIndex])
	return filter, nil
}
