package server

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/akrylysov/pogreb"
	"github.com/aviddiviner/go-murmur"
	"github.com/bbalet/stopwords"
	pb "github.com/disney/quanta/grpc"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/golang/protobuf/ptypes/wrappers"
	"github.com/steakknife/bloomfilter"
	"golang.org/x/text/unicode/norm"
	"hash"
	"hash/fnv"
	"io"
	"log"
	"regexp"
	"time"
)

const (
	maxElements = 100
	probCollide = 0.0000001
)

var (
	wordSegmenter = regexp.MustCompile(`[\pL\p{Mc}\p{Mn}\p{Nd}-_']+`)
)

// StringSearch service state.
type StringSearch struct {
	*EndPoint
	store *pogreb.DB
}

// NewStringSearch - Construct server side state for search service.
func NewStringSearch(endPoint *EndPoint) (*StringSearch, error) {

	db, err := pogreb.Open(endPoint.dataDir+"/"+"search.dat", nil)
	if err != nil {
		return nil, err
	}

	log.Println("Pre-warming  string search cache.")
	start := time.Now()
	count := 0
	it := db.Items()
	for {
		_, _, err := it.Next()
		if err != nil {
			if err != pogreb.ErrIterationDone {
				return nil, err
			}
			break
		}
		count++
	}
	elapsed := time.Since(start)
	log.Printf("Cache initialization complete %d items loaded in %s.\n", count, elapsed)

	e := &StringSearch{EndPoint: endPoint, store: db}
	pb.RegisterStringSearchServer(endPoint.server, e)
	return e, nil
}

// Shutdown search service.
func (m *StringSearch) Shutdown() {

	if m.store != nil {
		m.store.Sync()
		m.store.Close()
	}
}

// BatchIndex - Insert a new batch of searchable strings.
func (m *StringSearch) BatchIndex(stream pb.StringSearch_BatchIndexServer) error {

	for {
		sv, err := stream.Recv()
		if err == io.EOF {
			m.store.Sync()
			return stream.SendAndClose(&empty.Empty{})
		}
		if err != nil {
			return err
		}
		str := sv.GetValue()
		if sv == nil || str == "" {
			return fmt.Errorf("String value must not be empty")
		}

		// Key is hash of original string
		hashVal := uint64(murmur.MurmurHash2([]byte(str), 0))
		key := make([]byte, 8)
		binary.LittleEndian.PutUint64(key, hashVal)

		if found, err := m.store.Has(key); err != nil {
			return err
		} else if found {
			continue
		}

		// Construct bloom filter sans stopwords
		bloomFilter, err := constructBloomFilter(str)
		if err != nil {
			return err
		}

		bfBuf, err := bloomFilter.MarshalBinary()
		if err != nil {
			return err
		}

		if err := m.store.Put(key, bfBuf); err != nil {
			return err
		}
	}
}

// Search - Execute a text search.
func (m *StringSearch) Search(searchStr *wrappers.StringValue, stream pb.StringSearch_SearchServer) error {

	search := searchStr.GetValue()
	if searchStr == nil || search == "" {
		return fmt.Errorf("Search string must not be empty")
	}
	terms := parseTerms(search)

	hashedTerms := make([]hash.Hash64, len(terms))

	for i, v := range terms {
		hasher := fnv.New64a()
		hasher.Write(v)
		hashedTerms[i] = hasher
	}

	bloomFilter, err := bloomfilter.NewOptimal(maxElements, probCollide)
	if err != nil {
		return err
	}

	it := m.store.Items()
Top:
	for {
		stringHash, val, err := it.Next()
		if err != nil {
			if err != pogreb.ErrIterationDone {
				return err
			}
			break
		}

		bloomFilter.UnmarshalBinary(val)

		// Perform "and" comparison. Item will be selected if all terms are contained.
		for _, v := range hashedTerms {
			if !bloomFilter.Contains(v) {
				continue Top
			}
		}

		// return the hash of the original string value
		v := binary.LittleEndian.Uint64(stringHash[:8])
		if err := stream.Send(&wrappers.UInt64Value{Value: v}); err != nil {
			return err
		}
	}
	return nil
}

func parseTerms(content string) [][]byte {

	cleanStr := stopwords.CleanString(content, "en", true)
	c := norm.NFC.Bytes([]byte(cleanStr))
	c = bytes.ToLower(c)
	return wordSegmenter.FindAll(c, -1)
}

func constructBloomFilter(content string) (*bloomfilter.Filter, error) {

	words := parseTerms(content)

	// Construct bloom filter sans stopwords
	//bloomFilter, err := bloomfilter.NewOptimal(uint64(len(words)), probCollide)
	bloomFilter, err := bloomfilter.NewOptimal(uint64(maxElements), probCollide)
	if err != nil {
		return nil, err
	}

	for _, v := range words {
		hasher := fnv.New64a()
		hasher.Write(v)
		bloomFilter.Add(hasher)
	}
	return bloomFilter, nil
}
