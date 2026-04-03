/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.s3_data_lake

import io.airbyte.cdk.load.command.DefaultDestinationCatalogFactory
import io.airbyte.cdk.load.command.DestinationCatalog
import io.airbyte.cdk.load.command.DestinationStreamFactory
import io.airbyte.cdk.load.command.NamespaceMapper
import io.airbyte.cdk.load.schema.TableNameResolver
import io.airbyte.protocol.models.v0.ConfiguredAirbyteCatalog
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Primary
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton

/**
 * Replaces the default catalog factory to handle null generationId/minimumGenerationId/syncId.
 * Some Airbyte platform versions do not send these fields for custom connectors.
 */
@Factory
class S3DataLakeCatalogFactory {
    @Requires(property = "airbyte.connector.operation", notEquals = "check")
    @Singleton
    @Primary
    fun syncCatalog(
        catalog: ConfiguredAirbyteCatalog,
        streamFactory: DestinationStreamFactory,
        tableNameResolver: TableNameResolver,
        namespaceMapper: NamespaceMapper,
    ): DestinationCatalog {
        // Patch null protocol fields before the stream factory processes them
        catalog.streams.forEach { stream ->
            stream.generationId = stream.generationId ?: 1
            stream.minimumGenerationId = stream.minimumGenerationId ?: 0
            stream.syncId = stream.syncId ?: 1
        }

        val mappedDescriptors =
            catalog.streams.map { namespaceMapper.map(it.stream.namespace, it.stream.name) }.toSet()
        val names = tableNameResolver.getTableNameMapping(mappedDescriptors)

        require(names.size == catalog.streams.size) {
            "Invariant violation: An incomplete table name mapping was generated."
        }

        return DestinationCatalog(
            streams =
                catalog.streams.map {
                    val key = namespaceMapper.map(it.stream.namespace, it.stream.name)
                    streamFactory.make(it, names[key]!!)
                }
        )
    }
}
