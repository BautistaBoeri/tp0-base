package common

import (
	"encoding/csv"
	"io"
)

// BetReader es un wrapper sobre csv.Reader que mantiene el estado
// de las apuestas leídas que no cupieron en el batch anterior.
type BetReader struct {
	reader      *csv.Reader
	leftoverBet *Bet
}

// NewBetReader crea una nueva instancia de BetReader
func NewBetReader(r *csv.Reader) *BetReader {
	return &BetReader{
		reader:      r,
		leftoverBet: nil,
	}
}

// ReadBatch lee un bloque de apuestas desde el archivo CSV empaquetado.
// El batch cortará si alcanza 'maxAmount' O si excedería los 8000 bytes.
func (br *BetReader) ReadBatch(maxAmount int) ([]Bet, error) {
	var batch []Bet
	currentBatchSize := 3 // 3 bytes de header del protocolo de batch

	// 1. Si nos quedó una apuesta del batch anterior que no entraba, la agregamos primero
	if br.leftoverBet != nil {
		betSize := 2 + len(br.leftoverBet.FirstName) + len(br.leftoverBet.LastName) + len(br.leftoverBet.Document) + len(br.leftoverBet.Birthdate) + len(br.leftoverBet.Number) + 4
		batch = append(batch, *br.leftoverBet)
		currentBatchSize += betSize
		br.leftoverBet = nil // Ya la usamos, la limpiamos
	}

	for i := len(batch); i < maxAmount; i++ {
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

			// Calculamos el tamaño: 2 bytes de largo + la data + comas
			betSize := 2 + len(bet.FirstName) + len(bet.LastName) + len(bet.Document) + len(bet.Birthdate) + len(bet.Number) + 4

			// Si al sumar esta apuesta nos pasamos de seguridad (~8KB), la guardamos en la instancia para el PRÓXIMO batch
			if currentBatchSize+betSize > 8000 {
				br.leftoverBet = &bet
				// Retornamos nil porque no hay error, el único motivo de corte es que el batch está lleno
				return batch, nil
			}

			// Si entra, la metemos y seguimos
			batch = append(batch, bet)
			currentBatchSize += betSize
		}
	}

	return batch, nil
}
