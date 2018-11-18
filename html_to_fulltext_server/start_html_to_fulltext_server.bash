#!/usr/bin/env bash
gunicorn -b localhost:7295 html_to_fulltext:api --reload
