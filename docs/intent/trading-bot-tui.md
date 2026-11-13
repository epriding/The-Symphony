# Trading bot TUI — confirmed intent

Status: Confirmed by the user during the feature discovery interview.

## Outcome

Create a terminal user interface (TUI) that answers: “Is the bot working as
intended, and does anything need my attention?” Users should be able to
understand the bot without piecing together information from logs, commands,
and files.

## Users and shared context

The interface serves the human operator and AI agents. Both should have access
to the same bot state, errors, and diagnostic context. Agent findings should be
visible in the interface so the operator can follow the investigation in one
place.

## Why now

Early interactions with the bot exposed the frustration of gathering scattered
information to understand its behavior.

## Essential views

- An overview of bot health and all strategies, highlighting anything needing
  attention.
- A detail view for each strategy showing current trades, activity, and errors.
- A clear label identifying whether each strategy is paper trading or live
  trading with real funds.

## Essential controls

Provide basic start, pause, and stop controls. Their exact meaning and scope
must be defined before implementation.

## Success and operating constraints

- The operator can quickly understand the bot's status, its active strategies
  and trades, and whether anything needs attention.
- The TUI is usable from both the machine running the bot and the operator's
  laptop; both are equally important access points.
- Opening or closing the TUI never interrupts trading. The bot operates
  independently of the interface's lifecycle.
- The first version focuses on these essentials and can grow from there.

## Out of scope for the first version

- Editing strategy settings through the TUI.
- Building the agent's error reproduction, repair, or deployment workflow.
  Such work may involve the operator's wider coding setup.
- Defining agent autonomy or approval policies for that repair workflow.

## Decisions left for specification

This document records intent, not an implementation design. The connection
method for laptop access, agent interaction mechanism, and exact semantics of
start, pause, and stop remain to be specified against the existing bot.
