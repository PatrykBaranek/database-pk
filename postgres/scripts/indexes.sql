CREATE INDEX idx_calls_call_date ON calls (call_date);
CREATE INDEX idx_contacts_calls_call_id ON Contacts_Calls (call_id);
CREATE INDEX idx_contacts_calls_contact_id ON Contacts_Calls (contact_id);
CREATE INDEX idx_contacts_first_name ON contacts USING hash (first_name);
CREATE INDEX idx_contacts_phone_number ON contacts (phone_number);
CREATE INDEX idx_contacts_email ON contacts (email);