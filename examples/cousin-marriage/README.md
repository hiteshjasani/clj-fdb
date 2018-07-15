# Cousin Marriage Leaderboard

It turns out that marrying cousins is a thing in different parts of
the world.  Let's use this dataset provided by fivethirtyeight to make
a leaderboard.

We'll load the data into FoundationDB and then query our leaderboard.
If all goes well, we'll be able to see a leaderboard of countries
sorted from highest cousin marriage percentage to lowest.

Why did we make this example?  Besides the obvious entertainment
value, it showcases how to do sorting in fdb.  Fdb keys are sorted
lexicographically and that's the only option.  So it requires
applications to properly encode their data if they want various sorted
views.

## Run

    clj -m app.core

## References

Cousin marriage data made available from [fivethirtyeight][cousin-marriage-data].

[cousin-marriage-data]: https://github.com/fivethirtyeight/data/blob/master/cousin-marriage/cousin-marriage-data.csv
