package common

import (
	"encoding/csv"
	"io"
)

// BetReader es un wrapper sobre csv.Reader
type BetReader struct {
	reader *csv.Reader
}

// NewBetReader crea una nueva instancia de BetReader
func NewBetReader(r *csv.Reader) *BetReader {
	return &BetReader{
		reader: r,
	}
}

// ReadBatch lee un bloque de apuestas desde el archivo CSV empaquetado.
// El batch cortará si alcanza 'maxAmount' o el fin del archivo.
func (br *BetReader) ReadBatch(maxAmount int) ([]Bet, error) {
	var batch []Bet

	for i := 0; i < maxAmount; i++ {
		record, err := br.reader.Read()
		if err != nil {
			if err == io.EOF {
				return batch, io.EOF
			}
			return batch, err
		}

		if len(record) >= 5 {
			bet := Bet{
				FirstName: record[0],
				LastName:  record[1],
				Document:  record[2],
				Birthdate: record[3],
				Number:    record[4],
			}
			batch = append(batch, bet)
		}
	}

	return batch, nil
}
