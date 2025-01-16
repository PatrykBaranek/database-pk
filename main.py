from common.plotting.plotting import plot_result
from postgres.database.postgres_executor import PostgresExecutor
from postgres.perf_test.test_query import measure_postgres_times
from mongo.database.mongodb_executor import MongoDBExecutor
from mongo.perf_test.test_query import measure_mongo_times

def main():
    postgres_executor = PostgresExecutor('postgres', 'postgres', 'postgres', 'localhost', '5433')
    postgres, postgres_w_idx = measure_postgres_times(postgres_executor)

    mongo_service = MongoDBExecutor('mongo', 'localhost', '27017')
    mongo_service.delete_all_collections()
    mongo_service.create_collections()
    mongo, mongo_w_idx = measure_mongo_times(mongo_service)

    plot_result(postgres['insert'], mongo['insert'], 'Postgres vs MongoDB insert')
    plot_result(postgres['update'], mongo['update'], 'Postgres vs MongoDB update')
    plot_result(postgres['select_calls_by_date'], mongo['select_calls_by_date'], 'Postgres vs MongoDB select calls by date')
    plot_result(postgres['select_calls_by_participants'], mongo['select_calls_by_participants'], 'Postgres vs MongoDB select calls by participants')
    plot_result(postgres['select_groups_with_john'], mongo['select_groups_with_john'], 'Postgres vs MongoDB select groups with John')
    plot_result(postgres['select_phone_plus_one'], mongo['select_phone_plus_one'], 'Postgres vs MongoDB select phone plus one')
    plot_result(postgres['select_email_org'], mongo['select_email_org'], 'Postgres vs MongoDB select email org')
    plot_result(postgres['select_phone_plus_one_limit'], mongo['select_phone_plus_one_limit'], 'Postgres vs MongoDB select phone plus one limit')
    plot_result(postgres['select_email_org_limit'], mongo['select_email_org_limit'], 'Postgres vs MongoDB select email org limit')
    plot_result(postgres['delete'], mongo['delete'], 'Postgres vs MongoDB delete')

    plot_result(postgres_w_idx['insert'], mongo_w_idx['insert'], 'Postgres vs MongoDB with indexes insert')
    plot_result(postgres_w_idx['update'], mongo_w_idx['update'], 'Postgres vs MongoDB with indexes update')
    plot_result(postgres_w_idx['select_calls_by_date'], mongo_w_idx['select_calls_by_date'], 'Postgres vs MongoDB with indexes select calls by date')
    plot_result(postgres_w_idx['select_calls_by_participants'], mongo_w_idx['select_calls_by_participants'], 'Postgres vs MongoDB with indexes select calls by participants')
    plot_result(postgres_w_idx['select_groups_with_john'], mongo_w_idx['select_groups_with_john'], 'Postgres vs MongoDB with indexes select groups with John')
    plot_result(postgres_w_idx['select_phone_plus_one'], mongo_w_idx['select_phone_plus_one'], 'Postgres vs MongoDB with indexes select phone plus one')
    plot_result(postgres_w_idx['select_email_org'], mongo_w_idx['select_email_org'], 'Postgres vs MongoDB with indexes select email org')
    plot_result(postgres_w_idx['select_phone_plus_one_limit'], mongo_w_idx['select_phone_plus_one_limit'], 'Postgres vs MongoDB with indexes select phone plus one limit')
    plot_result(postgres_w_idx['select_email_org_limit'], mongo_w_idx['select_email_org_limit'], 'Postgres vs MongoDB with indexes select email org limit')
    plot_result(postgres_w_idx['delete'], mongo_w_idx['delete'], 'Postgres vs MongoDB with indexes delete')

if __name__ == "__main__":
    main()