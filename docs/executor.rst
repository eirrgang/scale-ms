====================
Execution middleware
====================

Executable graphs or graph segments produced by client software are dispatched
and translated for execution on managed computing resources.

.. uml:: diagrams/runtime_interface_sequence.puml

Simple work
===========

A single node of work may be executed immediately in the Worker Context,
or not at all (if the local checkpoint state is already final).

.. uml:: diagrams/runtime_immediate_sequence.puml

Deferred execution
==================

Multiple graph nodes may be received in the same packet of work, or asynchronously.
The executor may locally manage dependencies to optimize execution and data placement.

.. uml:: diagrams/runtime_deferred_sequence.puml
