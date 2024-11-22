CREATE INDEX idx_contacts_email ON Contacts USING hash (email);
CREATE INDEX idx_contacts_last_name ON Contacts USING hash (last_name);
CREATE INDEX idx_groups_name ON Groups USING hash (name);