package main

import (
	"database/sql"
)

type ParcelStore struct {
	db *sql.DB
}

func NewParcelStore(db *sql.DB) ParcelStore {
	return ParcelStore{db: db}
}

func (s ParcelStore) Add(p Parcel) (int, error) {
	res, err := s.db.Exec("insert into parcel (client, status, address, created_at)"+
		"values (:client, :status, :address, :created_at)",
		sql.Named("client", p.Client),
		sql.Named("status", p.Status),
		sql.Named("address", p.Address),
		sql.Named("created_at", p.CreatedAt),
	)
	if err != nil {
		return 0, err
	}

	number, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}

	return int(number), nil
}

func (s ParcelStore) Get(number int) (Parcel, error) {
	row := s.db.QueryRow("select number, client, status, address, created_at "+
		" from parcel where number = :number", sql.Named("number", number))

	p := Parcel{}
	if err := row.Scan(&p.Number, &p.Client, &p.Status, &p.Address, &p.CreatedAt); err != nil {
		return Parcel{}, err
	}

	return p, nil
}

func (s ParcelStore) GetByClient(client int) ([]Parcel, error) {
	rows, err := s.db.Query("select number, client, status, address, created_at "+
		" from parcel where client = :client", sql.Named("client", client))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var res []Parcel
	for rows.Next() {
		p := Parcel{}
		if err := rows.Scan(&p.Number, &p.Client, &p.Status, &p.Address, &p.CreatedAt); err != nil {
			return nil, err
		}
		res = append(res, p)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}

	return res, nil
}

func (s ParcelStore) SetStatus(number int, status string) error {
	_, err := s.db.Exec("update parcel set status = :status where number = :number",
		sql.Named("status", status),
		sql.Named("number", number),
	)
	if err != nil {
		return err
	}

	return nil
}

func (s ParcelStore) SetAddress(number int, address string) error {
	_, err := s.db.Exec("update parcel set address = :address "+
		" where status = :status and number = :number",
		sql.Named("address", address),
		sql.Named("status", ParcelStatusRegistered),
		sql.Named("number", number),
	)

	if err != nil {
		return err
	}

	return nil
}

func (s ParcelStore) Delete(number int) error {
	_, err := s.db.Exec("delete from parcel "+
		" where status = :status and number = :number",
		sql.Named("status", ParcelStatusRegistered),
		sql.Named("number", number),
	)

	if err != nil {
		return err
	}

	return nil
}
