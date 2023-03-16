#!/usr/bin/env python3

import json
import os
from openlineage.client import OpenLineageClient
import rich_click as click
from rich_click import style

client = OpenLineageClient.from_environment()


def print_client_info():
    print()
    match (client.transport.kind):
        case "console":
            print(
                "No backend configured, outputting to "
                + style("console", fg="red", bold=True)
                + "."
            )
        case "http":
            print(
                "Sending events via "
                + style("http", fg="red", bold=True)
                + " to "
                + style(client.transport.url, fg="yellow")
                + "/"
                + style(client.transport.endpoint, fg="yellow")
                + "."
            )
        case "kafka":
            if client.transport.flush == True:
                flush = "on"
            else:
                flush = "off"

            print(
                "Sending events via "
                + style("kafka", fg="red", bold=True)
                + " to the "
                + style(client.transport.topic, fg="yellow")
                + " topic with flush "
                + style(flush, fg="yellow")
                + "."
            )
        case other:
            print(
                "Transport "
                + style(client.transport.kind, fg="red")
                + " not recognized."
            )
    print()


@click.command()
@click.option(
    "-e",
    "--echo",
    is_flag=True,
    help="Echo each lineage event to the console.",
)
@click.argument('file')
def send(echo, file):
    """Send OpenLineage events to a backend from a file"""

    print_client_info()

    with open(file) as user_file:
        file_contents = user_file.read()

    ol_events = json.loads(file_contents)

    for ol_event in ol_events:
        event_type = ol_event["eventType"]
        job_name = ol_event["job"]["namespace"] + "." + ol_event["job"]["name"]
        click.echo(
            style(job_name, fg="yellow", bold=True)
            + " -> "
            + style(event_type, fg="red", bold=True)
        )
        client.transport.emit(ol_event)
        if echo:
            print(ol_event)


@click.command()
def config():
    """Get information on OpenLineage client configuration"""
    print_client_info()


@click.group()
@click.version_option("1.0.0")
def main():
    pass


main.add_command(send)
main.add_command(config)

if __name__ == "__main__":
    main()
