from common.plotting.plotting import plot_result
from postgres.database.postgres_executor import PostgresExecutor
from postgres.perf_test.test_query import measure_postgres_times
from mongo.database.mongodb_executor import MongoDBExecutor
from mongo.perf_test.test_query import measure_mongo_times


# ['insert', 'update', 'select_all', 'select_by_id', 'select_by_first_name', 'select_by_last_name', 'delete']
def main():
    postgres_executor = PostgresExecutor('postgres', 'postgres', 'postgres', 'localhost', '5433')
    postgres_executor.drop_all()
    postgres_executor.create_tables()
    postgres, postgres_w_idx = measure_postgres_times(postgres_executor)

    mongo_service = MongoDBExecutor('Performance', 'localhost', '27017')
    mongo_service.create_collections()
    mongo, mongo_w_idx = measure_mongo_times(mongo_service)

    plot_result(postgres['insert'], mongo['insert'], 'Postgres vs MongoDB insert')
    plot_result(postgres['update'], mongo['update'], 'Postgres vs MongoDB update')
    plot_result(postgres['select_all'], mongo['select_all'], 'Postgres vs MongoDB select all')
    plot_result(postgres['select_by_id'], mongo['select_by_id'], 'Postgres vs MongoDB select by id')
    plot_result(postgres['select_by_first_name'], mongo['select_by_first_name'], 'Postgres vs MongoDB select by first name')
    plot_result(postgres['select_by_last_name'], mongo['select_by_last_name'], 'Postgres vs MongoDB select by last name')
    plot_result(postgres['delete'], mongo['delete'], 'Postgres vs MongoDB delete')

    plot_result(postgres_w_idx['insert'], mongo_w_idx['insert'], 'Postgres vs MongoDB with indexes insert')
    plot_result(postgres_w_idx['update'], mongo_w_idx['update'], 'Postgres vs MongoDB with indexes update')
    plot_result(postgres_w_idx['select_all'], mongo_w_idx['select_all'], 'Postgres vs MongoDB with indexes select all')
    plot_result(postgres_w_idx['select_by_id'], mongo_w_idx['select_by_id'], 'Postgres vs MongoDB with indexes select by id')
    plot_result(postgres_w_idx['select_by_first_name'], mongo_w_idx['select_by_first_name'], 'Postgres vs MongoDB with indexes select by first name')
    plot_result(postgres_w_idx['select_by_last_name'], mongo_w_idx['select_by_last_name'], 'Postgres vs MongoDB with indexes select by last name')
    plot_result(postgres_w_idx['delete'], mongo_w_idx['delete'], 'Postgres vs MongoDB with indexes delete')

if __name__ == "__main__":
    main()