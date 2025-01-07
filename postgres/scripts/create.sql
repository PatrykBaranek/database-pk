CREATE TABLE Contacts (
    id SERIAL PRIMARY KEY,
    first_name VARCHAR(255) NOT NULL,
    last_name VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL,
    phone_number VARCHAR(30),
    address TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE Groups (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE Contact_Groups (
    contact_id INT NOT NULL,
    group_id INT NOT NULL,
    PRIMARY KEY (contact_id, group_id),
    FOREIGN KEY (contact_id) REFERENCES Contacts(id) ON DELETE CASCADE,
    FOREIGN KEY (group_id) REFERENCES Groups(id) ON DELETE CASCADE
);

CREATE TABLE Calls (
    id SERIAL PRIMARY KEY,
    duration INT NOT NULL,
    call_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE Contacts_Calls (
    call_id INT NOT NULL,
    contact_id INT NOT NULL,
    PRIMARY KEY (call_id, contact_id),
    FOREIGN KEY (call_id) REFERENCES Calls(id) ON DELETE CASCADE,
    FOREIGN KEY (contact_id) REFERENCES Contacts(id) ON DELETE CASCADE
);