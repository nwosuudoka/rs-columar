use xxhash_rust::xxh3;

pub const DOC_MAGIC: &[u8; 6] = b"MIDOC1";
pub const DOC_HEADER_SIZE: usize = 32; // magic (6) + total_data_size (8) + entry_count (4)
pub const DOC_VERSION: u8 = 1;

pub fn hash_string(s: &str) -> u64 {
    xxh3::xxh3_64(s.as_bytes())
}

pub(crate) fn process_string(s: &str) -> Vec<u64> {
    s.split(" ").map(|s| xxh3::xxh3_64(s.as_bytes())).collect()
}

pub fn sliding_ngram_hash(tokens: &[u64], win_sz: u8, max_end_win_sz: u8) -> Vec<u64> {
    let n = tokens.len();

    // Match Go: return as-is for empty or single-element
    if n == 0 {
        return vec![];
    }
    if n == 1 {
        return tokens.to_vec(); // return [token], not hashed
    }

    let mut vec = Vec::with_capacity((win_sz * 8) as usize);

    let win_sz = win_sz as usize;
    let max_end = max_end_win_sz as usize;

    // If input smaller than window, hash entire slice
    if n < win_sz {
        return vec![combine_hashes(tokens, &mut vec)];
    }

    // Compute number of results: n - max_end + 1
    let num_results = n.saturating_sub(max_end).saturating_add(1);
    if num_results == 0 {
        return vec![];
    }

    let mut result = Vec::with_capacity(num_results);

    for i in 0..num_results {
        let end = std::cmp::min(i + win_sz, n);
        let window = &tokens[i..end];
        result.push(combine_hashes(window, &mut vec));
    }

    result
}

fn combine_hashes(li: &[u64], vec: &mut Vec<u8>) -> u64 {
    let size = li.len() * 8;
    if vec.capacity() < size {
        vec.reserve(size);
    }
    for x in li {
        vec.extend_from_slice(&x.to_le_bytes());
    }
    let result = vec.as_slice();
    xxh3::xxh3_64(result)
}
