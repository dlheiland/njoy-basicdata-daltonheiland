CREATE TABLE campaigns.message_blast(
    profile_id text PRIMARY KEY,
    clicks map<time,text>,
    delivery_status text,
    device text,
    message_id bigint,
    message_revenue text,
    opens list<time>,
    blast_id bigint,
    send_time timestamp,
    );