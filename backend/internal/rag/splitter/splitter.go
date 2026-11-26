// 递归字符切分：分隔符优先级 "\n\n" → "\n" → "。" → "." → " " → ""，
// 带重叠（overlap）保证语义连续 —— 这是 RAG 检索质量的第一决定因素。
package splitter

import (
	"strings"
)

// Splitter 分块器。
type Splitter struct {
	ChunkSize   int // 块大小（字符）
	ChunkOverlap int // 重叠（字符）
}

// New 构造。
func New(size, overlap int) *Splitter {
	if size <= 0 {
		size = 512
	}
	if overlap < 0 || overlap >= size {
		overlap = size / 8
	}
	return &Splitter{ChunkSize: size, ChunkOverlap: overlap}
}

// Chunk 一个分块。
type Chunk struct {
	Seq     int    // 序号
	Content string // 内容
	Page    int    // 来源页码
}
var separators = []string{"\n\n", "\n", "。", ".", " "}
// Split 单页内容切分（标题感知：markdown 标题优先分段）。
func (s *Splitter) Split(content string, page int) []Chunk {
	var chunks []Chunk
	for _, seg := range splitByHeading(content) {
		chunks = append(chunks, s.recursiveSplit(seg, page, separators)...)
	}
	// 编号
	for i := range chunks {
		chunks[i].Seq = i
	}
	return chunks
}

// splitByHeading markdown 标题切分（# 开头行）。
func splitByHeading(content string) []string {
	lines := strings.Split(content, "\n")
	var segs []string
	var cur strings.Builder
	for _, line := range lines {
		if strings.HasPrefix(line, "#") && cur.Len() > 0 {
			segs = append(segs, cur.String())
			cur.Reset()
		}
		cur.WriteString(line + "\n")
	}
	if cur.Len() > 0 {
		segs = append(segs, cur.String())
	}
	return segs
}

// separators 分隔符优先级。


func (s *Splitter) recursiveSplit(text string, page int, seps []string) []Chunk {
    text = strings.TrimSpace(text)
    runes := []rune(text)
    if len(runes) == 0 {
        return nil
    }
    if len(runes) <= s.ChunkSize {
        return []Chunk{{Content: text, Page: page}}
    }

    for i, sep := range seps {
        if !strings.Contains(text, sep) {
            continue
        }

        var chunks []Chunk
        var pending []rune
        emit := func() {
            content := strings.TrimSpace(string(pending))
            if content != "" {
                chunks = append(chunks, Chunk{Content: content, Page: page})
            }
        }

        for _, part := range splitKeepSep(text, sep) {
            partRunes := []rune(part)
            if len(partRunes) > s.ChunkSize {
                // 先输出前面的内容，再用更细的分隔符处理长段。
                emit()
                pending = pending[:0]
                chunks = append(chunks, s.recursiveSplit(part, page, seps[i+1:])...)
                continue
            }

            if len(pending)+len(partRunes) > s.ChunkSize {
                emit()
                keep := min(s.ChunkOverlap, len(pending), s.ChunkSize-len(partRunes))
                copy(pending, pending[len(pending)-keep:])
                pending = pending[:keep]
            }
            pending = append(pending, partRunes...)
        }

        emit()
        return chunks
    }

    // 分隔符用完后，按字符数切分并保留重叠。
    var chunks []Chunk
    step := s.ChunkSize - s.ChunkOverlap
    for start := 0; start < len(runes); start += step {
        end := min(start+s.ChunkSize, len(runes))
        chunks = append(chunks, Chunk{Content: string(runes[start:end]), Page: page})
        if end == len(runes) {
            break
        }
    }
    return chunks
}

// splitKeepSep 按分隔符切分并保留分隔符在段尾。
func splitKeepSep(text, sep string) []string {
	if sep == "" {
		return []string{text}
	}
	parts := strings.Split(text, sep)
	out := make([]string, 0, len(parts))
	for i, p := range parts {
		if i < len(parts)-1 {
			p += sep
		}
		if strings.TrimSpace(p) != "" {
			out = append(out, p)
		}
	}
	return out
}
