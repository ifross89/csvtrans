package csvtrans

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
)

// RowTransformer is a function that transforms a CSV row.
// If an error is returned, the CSV transformation will terminate.
// If a row is to be skipped, return with outRow and err == nil
type RowTransformer func(i int, inRow []string) (outRow []string, err error)

// BufRowTransformer is similar to RowTransformer, but also takes a buffer which
// should be returned. This allows the output CSV to reuse a buffer between
// calls, to reduce garbage
type BufRowTransformer func(i int, inRow []string, outRow []string) ([]string, error)

// Creates a RowTransformer from a BufRowTransformer which reuses the same buffer.
// Note that when using a BufRowTransformer with this that every field must be
// set each iteration, or values will be repeated between rows.
// length is the number of columns in the output CSV
func MakeRowTransformer(length int, f BufRowTransformer) RowTransformer {
	buf := make([]string, length, length)
	return func(i int, inRow []string) ([]string, error) {
		return f(i, inRow, buf)
	}
}

// Run performs the transformation on the CSV given an input reader, output
// writer and a function to transform each row.
func Run(in io.Reader, out io.Writer, f RowTransformer) error {
	inCsv := csv.NewReader(in)
	outCsv := csv.NewWriter(out)

	defer outCsv.Flush()
	i := 0
	for {
		row, err := inCsv.Read()

		if err == io.EOF {
			return nil
		} else if err != nil {
			return fmt.Errorf("Error reading CSV row at index %d: %v", i, err)
		}

		transformed, err := f(i, row)
		if err != nil {
			return fmt.Errorf("Error transforming row at index %d: %v", i, err)
		}

		// Skip if row is nil and no error
		if transformed != nil {
			err = outCsv.Write(transformed)
			if err != nil {
				return fmt.Errorf("Error writing CSV row at index %d: %v", i, err)
			}
		}

		i += 1
	}
}

// RunFile is a wrapper around Run. This uses files as the input and output.
// Different filenames must be passed in to the function.
// If an output file already exists, it will be replaced.
func RunFile(inFile, outFile string, f RowTransformer) error {
	if inFile == outFile {
		return errors.New("inFile and outFile must be different")
	}

	in, err := os.Open(inFile) // Read access
	if err != nil {
		return fmt.Errorf("Error reading %s: %v", inFile, err)
	}
	defer in.Close()

	out, err := os.Create(outFile)
	if err != nil {
		return fmt.Errorf("Error opening %s for write: %v", outFile, err)
	}
	defer out.Close()

	err = Run(in, out, f)
	if err != nil {
		return err
	}
	return nil
}
