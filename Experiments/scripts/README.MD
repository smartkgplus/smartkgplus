To run tests similar to the ones in the paper, you must run the jar file once per client you are running. Then, use the following command per client.

java -jar [filename].jar -t true [Starting Fragment] [Client Directory] [Approach] [Output Directory] [No. Clients] [Client No.] [Query Load]
In the [Client Directory], there must be one subdirectory per query load entitled [Query Load].


Run_experiments.sh performs this step automatically and initiates the calls to the clients to start sending queries which executes run_clients.sh on the client-side

