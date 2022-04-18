import os
import click




@click.group(help='Generate and process base dbt models')
def dbt_generator():
    pass


@dbt_generator.command(help='Gennerate base models based on a .yml source')
def generate():
    print(f'Generating base model for table {table_name}')


if __name__ == '__main__':
    dbt_generator()