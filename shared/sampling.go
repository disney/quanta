package shared

import (
	"github.com/RoaringBitmap/roaring/roaring64"
	"math/rand"
	"sync"
	"time"
)

//
// PerformStratifiedSampling - Perform sampling on a group of bitmaps representing a data attribute.
// Each bitmap in the input represents a subgroup.  Per the stratified methodology, a percentage
// of each subgroup is sampled proportionally to the overall desired sample size.
// At the moment, only enumerated data attribute types are supported.
//
// Returns the sample set.
//
func PerformStratifiedSampling(input []*roaring64.Bitmap, samplePct float32) []*roaring64.Bitmap {

	result := make([]*roaring64.Bitmap, len(input))
	rg := make([]*rand.Rand, len(input))
	total := uint64(0)
	for i, v := range input {
		total += v.GetCardinality()
		result[i] = roaring64.NewBitmap()
		rg[i] = rand.New(rand.NewSource(time.Now().UnixNano()))

	}
	sampleCount := uint64(float64(total) * float64(samplePct) / 100)
	if total == 0 || sampleCount == 0 {
		return result
	}
	uniqueSets := make([]map[uint64]struct{}, len(input))
	setSizes := make([]uint64, len(input))
	var wg sync.WaitGroup
	for i, v := range input {
		wg.Add(1)
		go func(n int, bm *roaring64.Bitmap) {
			defer wg.Done()
			percentage := float64(bm.GetCardinality()) / float64(total)
			// Calculate proportional sample set size and allocate sized map
			setSizes[n] = uint64(float64(sampleCount) * percentage)
			uniqueSets[n] = make(map[uint64]struct{})
			// Populate uniqueSets with random index values
			for uint64(len(uniqueSets[n])) < setSizes[n] {
				uniqueSets[n][uint64(rg[n].Int63n(int64(setSizes[n])))] = struct{}{}
			}
			inputAsArray := input[n].ToArray()
			for k := range uniqueSets[n] {
				result[n].Add(inputAsArray[k])
			}
		}(i, v)
	}
	wg.Wait()
	return result
}
