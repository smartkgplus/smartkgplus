To run tests similar to the ones in the paper, you must run the jar file once per client you are running. Then, use the following command per client.

java -jar [filename].jar -t true [Starting Fragment] [Client Directory] [Approach] [Output Directory] [No. Clients] [Client No.] [Query Load]
In the [Client Directory], there must be one subdirectory per query load entitled [Query Load].

To full test setup used in the paper will be available soon on our website.

Run_experiments. performs this step automatically and inititate the calls to the clients to start sending queries

