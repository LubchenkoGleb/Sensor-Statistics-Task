Points to note:
- The program processes all files as a stream and keeps in memory only one chunk of input data along with accumulated statistics for each sensor;
- One of the requirements is "Program should only use memory for its internal state (no disk, no database)", so we have a limitation that it will only work if the total number of sensors and accumulated statistics related to this sensor can fit into memory.
- The application uses mutable variable to collect statistics at two places. I believe that in the current scenario, the use of mutable state and variables is beneficial from the performance and code readability point of view. The mentioned places in the code are marked with comments that explain how this code could be rewritten in the pure immutable functional style.
- To run the app please execute the command `sbt run <input-data-path>`, to run unit tests execute `sbt test`
- The app is implemented and tested using Java17