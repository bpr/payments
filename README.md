**Payments Engine**

Approach:

- Read CSV a line at a time and implement described state machine
  - Bad data is handled by skipping the record.
  - It is assumed that all of the amounts concerned (available, held, total) must be non-negative .
  - Error reporting: every record skip, which occurs on an invalid record, is indicated by a **continue** and an
    **eprintln!** call. The instructions specified that all output should be to stdout, so I assume stderr is fine for
      error reporting.
  - Other invalid states are similarly skipped and accompanied by stderr reporting
- Questions: 
  - The instructions seemed to suggest that disputed transaction behavior is the same for deposits and withdrawals, which
    is questionable.
  

```
