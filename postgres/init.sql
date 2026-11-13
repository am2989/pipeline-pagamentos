CREATE TABLE accounts (
    account_id SERIAL PRIMARY KEY,
    customer_name VARCHAR(100) NOT NULL,
    email VARCHAR(150) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE payments (
    payment_id SERIAL PRIMARY KEY,
    account_id INTEGER REFERENCES accounts(account_id),
    card_number VARCHAR(20) NOT NULL,
    amount DECIMAL(10,2) NOT NULL,
    currency VARCHAR(3) DEFAULT 'EUR',
    status VARCHAR(20) DEFAULT 'PENDING',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE public.payments REPLICA IDENTITY FULL;

INSERT INTO accounts (customer_name, email) VALUES
('Ana Silva', 'ana@example.com'),
('Joao Costa', 'joao@example.com'),
('Maria Santos', 'maria@example.com');

INSERT INTO payments (account_id, card_number, amount, status) VALUES
(1, 'CARD-001', 25.50, 'APPROVED'),
(2, 'CARD-002', 120.00, 'APPROVED'),
(1, 'CARD-001', 45.00, 'PENDING'),
(1, 'CARD-001', 80.00, 'APPROVED'),
(3, 'CARD-003', 15.00, 'APPROVED');
