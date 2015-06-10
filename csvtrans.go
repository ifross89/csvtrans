package csvtrans

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
)

type RowTransformer func(i int, inRow []string) (outRow []string, err error)

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

		err = outCsv.Write(transformed)
		if err != nil {
			return fmt.Errorf("Error writing CSV row at index %d: %v", i, err)
		}
		i += 1
	}
}
