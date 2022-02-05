package shared

import (
	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"testing"
)

func TestStratifiedSampling(t *testing.T) {

	dat1, err := ioutil.ReadFile("./testdata/gender/1")
	require.Nil(t, err)
	dat2, err := ioutil.ReadFile("./testdata/gender/2")
	require.Nil(t, err)
	dat3, err := ioutil.ReadFile("./testdata/gender/3")
	require.Nil(t, err)

	input := make([]*roaring64.Bitmap, 3)
	input[0] = roaring64.NewBitmap()
	input[1] = roaring64.NewBitmap()
	input[2] = roaring64.NewBitmap()

	err = input[0].UnmarshalBinary(dat1)
	require.Nil(t, err)
	err = input[1].UnmarshalBinary(dat2)
	require.Nil(t, err)
	err = input[2].UnmarshalBinary(dat3)
	require.Nil(t, err)

	output := PerformStratifiedSampling(input, 1)
	assert.NotNil(t, output)
	assert.Equal(t, output[0].GetCardinality(), uint64(279853))
	assert.Equal(t, output[1].GetCardinality(), uint64(60502))
	assert.Equal(t, output[2].GetCardinality(), uint64(238708))

	// OR the bitmaps together to perform a simple random sample
	output2 := PerformStratifiedSampling([]*roaring64.Bitmap{roaring64.ParOr(0, input...)}, 1)
	assert.Equal(t, output2[0].GetCardinality(), uint64(579064))
}
