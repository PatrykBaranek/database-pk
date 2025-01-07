from common.plotting.plotting import plot_result
from postgres.database.postgres_executor import PostgresExecutor
from postgres.perf_test.test_query import measure_postgres_times


# ['insert', 'update', 'select_all', 'select_by_id', 'select_by_first_name', 'select_by_last_name', 'delete']
def main():
    postgres_service = PostgresExecutor('postgres', 'postgres', 'postgres', 'localhost', '5432')
    postgres_service.create_tables()
    postgres, postgres_w_idx = measure_postgres_times(postgres_service)
    mongo = []  # TODO: Measure MongoDB times

    plot_result(postgres['insert'], mongo, 'Postgres vs MongoDB insert')
    plot_result(postgres['update'], mongo, 'Postgres vs MongoDB update')
    plot_result(postgres['select_all'], mongo, 'Postgres vs MongoDB select all')
    plot_result(postgres['select_by_id'], mongo, 'Postgres vs MongoDB select by id')
    plot_result(postgres['select_by_first_name'], mongo, 'Postgres vs MongoDB select by first name')
    plot_result(postgres['select_by_last_name'], mongo, 'Postgres vs MongoDB select by last name')
    plot_result(postgres['delete'], mongo, 'Postgres vs MongoDB delete')

    plot_result(postgres_w_idx['insert'], mongo, 'Postgres vs MongoDB with indexes insert')
    plot_result(postgres_w_idx['update'], mongo, 'Postgres vs MongoDB with indexes update')
    plot_result(postgres_w_idx['select_all'], mongo, 'Postgres vs MongoDB with indexes select all')
    plot_result(postgres_w_idx['select_by_id'], mongo, 'Postgres vs MongoDB with indexes select by id')
    plot_result(postgres_w_idx['select_by_first_name'], mongo, 'Postgres vs MongoDB with indexes select by first name')
    plot_result(postgres_w_idx['select_by_last_name'], mongo, 'Postgres vs MongoDB with indexes select by last name')
    plot_result(postgres_w_idx['delete'], mongo, 'Postgres vs MongoDB with indexes delete')
