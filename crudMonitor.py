from datetime import datetime
from timeit import timeit
from faker import Faker
from db import MongoDBSingleton
import plotly.graph_objects as go


database = MongoDBSingleton()
db = database.get_db()

class CRUDPerformanceTracker:
    def __init__(self):
        self.timing_data = {
            'ADD': [],
            'READ': [],
            'UPDATE': [],
            'DELETE': []
        }

    def measure_add_contacts(self, add_contacts_func, num_records):
        """
        Measure time to add contacts
        :param add_contacts_func: Function to add contacts
        :param num_records: Number of records to add
        """
        execution_time = timeit(
            lambda: add_contacts_func(num_records),
            number=1
        )
        self.timing_data['ADD'].append(execution_time)
        print(f"Time to add {num_records} contacts: {execution_time:.2f} seconds")
        return execution_time

    def measure_read_contacts(self):
        """Measure time to read all contacts"""
        execution_time = timeit(
            lambda: list(db.contacts.find()),
            number=1
        )
        self.timing_data['READ'].append(execution_time)
        print(f"Time to read contacts: {execution_time:.2f} seconds")
        return execution_time

    def measure_update_contacts(self, num_records):
        """
        Measure time to update contacts
        :param num_records: Number of records to update
        """
        execution_time = timeit(
            lambda: db.contacts.update_one(
                {'first_name': f'FirstName{0}'},
                {'$set': {'phone_number': '+48987654321'}}
            ),
            number=num_records
        )
        self.timing_data['UPDATE'].append(execution_time)
        print(f"Time to update {num_records} contacts: {execution_time:.2f} seconds")
        return execution_time

    def measure_delete_all_contacts(self):
        """Measure time to delete all contacts"""
        execution_time = timeit(
            lambda: db.contacts.delete_many({}),
            number=1
        )
        self.timing_data['DELETE'].append(execution_time)
        print(f"Time to delete all contacts: {execution_time:.2f} seconds")
        return execution_time

    def measure_delete_single_contact(self, num_records):
        """
        Measure time to delete single contacts
        :param num_records: Number of records to delete
        """
        execution_time = timeit(
            lambda: db.contacts.delete_one({'first_name': f'FirstName{0}'}),
            number=num_records
        )
        self.timing_data['DELETE'].append(execution_time)
        print(f"Time to delete {num_records} contacts: {execution_time:.2f} seconds")
        return execution_time

    def plot_timing_data(self):
        """Create interactive plots of CRUD operation timings"""
        # Create subplots
        fig = go.Figure()

        colors = {
            'ADD': 'rgb(49, 130, 189)',  # Blue
            'READ': 'rgb(50, 171, 96)',  # Green
            'UPDATE': 'rgb(255, 127, 14)',  # Orange
            'DELETE': 'rgb(219, 64, 82)'  # Red
        }

        # Add bar chart
        for i, (operation, times) in enumerate(self.timing_data.items()):
            if times:  # Only plot if there's data
                fig.add_trace(go.Bar(
                    name=operation,
                    y=times,
                    marker_color=colors[operation],
                    text=[f"{t:.3f}s" for t in times],
                    textposition='auto',
                ))

        # Update layout
        fig.update_layout(
            title={
                'text': 'MongoDB CRUD Operations Performance',
                'y': 0.95,
                'x': 0.5,
                'xanchor': 'center',
                'yanchor': 'top',
                'font': dict(size=24)
            },
            yaxis_title='Execution Time (seconds)',
            xaxis_title='Operation Number',
            barmode='group',
            bargap=0.15,
            bargroupgap=0.1,
            template='plotly_white',
            showlegend=True,
            legend=dict(
                yanchor="top",
                y=0.99,
                xanchor="right",
                x=0.99
            ),
            hoverlabel=dict(
                bgcolor="white",
                font_size=16,
                font_family="Rockwell"
            ),
        )

        # Add hover template
        fig.update_traces(
            hovertemplate="<br>".join([
                "<b>%{data.name}</b>",
                "Time: %{y:.4f} seconds",
                "<extra></extra>"
            ])
        )

        # Update axes
        fig.update_yaxes(
            gridcolor='lightgray',
            zeroline=True,
            zerolinecolor='black',
            zerolinewidth=1
        )

        fig.update_xaxes(
            gridcolor='lightgray',
            zeroline=True,
            zerolinecolor='black',
            zerolinewidth=1
        )

        return fig


def main():

    # Create performance tracker
    tracker = CRUDPerformanceTracker()

    def add_contacts(num_records):
        contacts = []
        for i in range(num_records):
            faker = Faker()
            contact = {
                'first_name': f"FirstName{i}",
                'last_name': faker.last_name(),
                'email': faker.email(),
                'phone_number': faker.phone_number(),
                'address': faker.address(),
                'created_at': datetime.now()
            }
            contacts.append(contact)
        db.contacts.insert_many(contacts)

    # Perform operations
    tracker.measure_add_contacts(add_contacts, 10000)
    tracker.measure_read_contacts()
    tracker.measure_update_contacts(100)
    tracker.measure_delete_all_contacts()

    # Generate and show the plot
    fig = tracker.plot_timing_data()
    fig.show()

if __name__ == "__main__":
    main()