.. :changelog:

History
-------

0.3 (2012-08-29)
++++++++++++++++++

* Limited scope to block parsing, **not** responsible for re-structuring.
* Replace consumers with threaded source(s) --> channel -> sink.
* Add throttling.
* Store tracking offsets in sqlite db.
* Split off all processing logic to sinks (e.g. file, debug, python, socket,
  etc) which can (e.g. for file or socket sinks) be handled by any language.

0.2 (2012-05-28)
++++++++++++++++++

* Hope you like it.

0.1 (2012-01-01)
++++++++++++++++++

* Its alive!
