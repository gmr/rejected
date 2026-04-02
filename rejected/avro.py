
    # Avro support

    def _avro_schema(self, message_type: str) -> dict:
        """Return the parsed Avro schema for the given message type, loading
        and caching it on first access.

        :param str message_type: The AMQP ``type`` property value
        :rtype: dict

        """
        if message_type not in self._avro_schemas:
            self.logger.debug('Loading Avro schema for %s', message_type)
            self._avro_schemas[message_type] = self._load_avro_schema(
                message_type
            )
        return self._avro_schemas[message_type]

    def _load_avro_schema(self, message_type: str) -> dict:
        """Load and return the Avro schema for the given message type.

        If ``schema_uri_format`` is set in the consumer's config, the URI
        is built by calling ``schema_uri_format.format(message_type)`` and
        the scheme determines how the schema is fetched:

        - ``file:///path/to/schemas/{0}.avsc`` -- read from disk
        - ``http://`` / ``https://`` -- fetched via HTTP GET

        If ``schema_uri_format`` is not set, override this method to provide
        your own schema loading logic.

        :param str message_type: The AMQP ``type`` property value
        :rtype: dict
        :raises: NotImplementedError if schema_uri_format is not configured
                 and this method has not been overridden

        """
        uri_format = (
            self._process.consumer_config.schema_uri_format
            if self._process
            else None
        )
        if not uri_format:
            raise NotImplementedError(
                'Set schema_uri_format in consumer config or override '
                '_load_avro_schema to provide Avro schemas'
            )
        uri = uri_format.format(message_type)
        self.logger.debug(
            'Loading Avro schema for %s from %s', message_type, uri
        )
        if uri.startswith('file://'):
            file_path = pathlib.Path(uri[7:])
            try:
                return json.loads(file_path.read_text())
            except FileNotFoundError:
                raise ConsumerException(
                    f'Missing Avro schema file: {file_path}'
                ) from None
        if uri.startswith(('http://', 'https://')):
            if not _requests:
                raise ConsumerException(
                    'requests is required for HTTP schema loading; '
                    'install rejected[avro]'
                )
            response = _requests.get(uri, timeout=30)
            if not response.ok:
                raise ConsumerException(
                    f'Failed to fetch Avro schema for {message_type}: '
                    f'HTTP {response.status_code}'
                )
            return response.json()
        raise ConsumerException(
            f'Unsupported schema URI scheme in {uri!r}; '
            f'use file:// or http(s)://'
        )
