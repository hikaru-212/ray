@PublicAPI(stability="alpha")
def read_sql(
    sql: str,
    connection_factory: Callable[[], Connection],
    *,
    sql_params: Optional[Any] = None,
    shard_keys: Optional[list[str]] = None,
    shard_hash_fn: str = "MD5",
    parallelism: int = -1,
    num_cpus: Optional[float] = None,
    num_gpus: Optional[float] = None,
    memory: Optional[float] = None,
    ray_remote_args: Optional[Dict[str, Any]] = None,
    concurrency: Optional[int] = None,
    override_num_blocks: Optional[int] = None,
) -> Dataset:
    """Read from a database that provides a
    `Python DB API2-compliant <https://peps.python.org/pep-0249/>`_ connector.

    .. note::

        Parallelism is supported by databases that support sharding. This means
        that the database needs to support all of the following operations:
        ``MOD``, ``ABS``, and ``CONCAT``.

        You can use ``shard_hash_fn`` to specify the hash function to use for sharding.
        The default is ``MD5``, but other common alternatives include ``hash``,
        ``unicode``, and ``SHA``.

        If the database does not support sharding, the read operation will be
        executed in a single task.

    Examples:

        For examples of reading from larger databases like MySQL and PostgreSQL, see
        :ref:`Reading from SQL Databases <reading_sql>`.

        .. testcode::

            import sqlite3

            import ray

            # Create a simple database
            connection = sqlite3.connect("example.db")
            connection.execute("CREATE TABLE movie(title, year, score)")
            connection.execute(
                \"\"\"
                INSERT INTO movie VALUES
                    ('Monty Python and the Holy Grail', 1975, 8.2),
                    ("Monty Python Live at the Hollywood Bowl", 1982, 7.9),
                    ("Monty Python's Life of Brian", 1979, 8.0),
                    ("Rocky II", 1979, 7.3)
                \"\"\"
            )
            connection.commit()
            connection.close()

            def create_connection():
                return sqlite3.connect("example.db")

            # Get all movies
            ds = ray.data.read_sql("SELECT * FROM movie", create_connection)
            # Get movies after the year 1980
            ds = ray.data.read_sql(
                "SELECT title, score FROM movie WHERE year >= 1980", create_connection
            )
            # Get the number of movies per year
            ds = ray.data.read_sql(
                "SELECT year, COUNT(*) FROM movie GROUP BY year", create_connection
            )

        .. testcode::
            :hide:

            import os
            os.remove("example.db")

    Args:
        sql: The SQL query to execute.
        connection_factory: A function that takes no arguments and returns a
            Python DB API2
            `Connection object <https://peps.python.org/pep-0249/#connection-objects>`_.
        sql_params: Parameters to bind to the SQL query. Use the placeholder style
            required by your database connector (per Python DB API2).
        shard_keys: The keys to shard the data by.
        shard_hash_fn: The hash function string to use for sharding. Defaults to "MD5".
            For other databases, common alternatives include "hash" and "SHA".
            This is applied to the shard keys.
        parallelism: This argument is deprecated. Use ``override_num_blocks`` argument.
        num_cpus: The number of CPUs to reserve for each parallel read worker.
        num_gpus: The number of GPUs to reserve for each parallel read worker. For
            example, specify `num_gpus=1` to request 1 GPU for each parallel read
            worker.
        memory: The heap memory in bytes to reserve for each parallel read worker.
        ray_remote_args: kwargs passed to :func:`ray.remote` in the read tasks.
        concurrency: The maximum number of Ray tasks to run concurrently. Set this
            to control number of tasks to run concurrently. This doesn't change the
            total number of tasks run or the total number of output blocks. By default,
            concurrency is dynamically decided based on the available resources.
        override_num_blocks: Override the number of output blocks from all read tasks.
            This is used for sharding when shard_keys is provided.
            By default, the number of output blocks is dynamically decided based on
            input data size and available resources. You shouldn't manually set this
            value in most cases.

    Returns:
        A :class:`Dataset` containing the queried data.
    """
    datasource = SQLDatasource(
        sql=sql,
        sql_params=sql_params,
        shard_keys=shard_keys,
        shard_hash_fn=shard_hash_fn,
        connection_factory=connection_factory,
    )
    if override_num_blocks and override_num_blocks > 1:
        if shard_keys is None:
            raise ValueError("shard_keys must be provided when override_num_blocks > 1")

        if not datasource.supports_sharding(override_num_blocks):
            raise ValueError(
                "Database does not support sharding. Please set override_num_blocks to 1."
            )

    return read_datasource(
        datasource,
        num_cpus=num_cpus,
        num_gpus=num_gpus,
        memory=memory,
        parallelism=parallelism,
        ray_remote_args=ray_remote_args,
        concurrency=concurrency,
        override_num_blocks=override_num_blocks,
    )
