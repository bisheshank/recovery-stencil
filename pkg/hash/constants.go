package hash

import (
	"dinodb/pkg/pager"
	"encoding/binary"
)

/////////////////////////////////////////////////////////////////////////////
////////////////////////// Low-level Constants //////////////////////////////
/////////////////////////////////////////////////////////////////////////////

const ROOT_PN int64 = 0
const PAGESIZE int64 = pager.Pagesize
const DEPTH_OFFSET int64 = 0
const DEPTH_SIZE int64 = binary.MaxVarintLen64
const NUM_KEYS_OFFSET int64 = DEPTH_OFFSET + DEPTH_SIZE
const NUM_KEYS_SIZE int64 = binary.MaxVarintLen64
const BUCKET_HEADER_SIZE int64 = DEPTH_SIZE + NUM_KEYS_SIZE
const ENTRYSIZE int64 = binary.MaxVarintLen64 * 2                         // int64 key, int64 value
const MAX_BUCKET_SIZE int64 = (PAGESIZE - BUCKET_HEADER_SIZE) / ENTRYSIZE // max number of entries that can live in a bucket
