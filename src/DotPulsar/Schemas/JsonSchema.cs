/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace DotPulsar.Schemas;

using System.Text.Json;
using DotPulsar.Abstractions;
using System.Buffers;
using System.Collections.Immutable;
using System.Text;

/// <summary>
/// Schema definition for JSON messages.
/// </summary>
public sealed class JsonSchema<TMessage> : ISchema<TMessage>
{
    public JsonSchema()
        => SchemaInfo = new SchemaInfo("Json", Array.Empty<byte>(), SchemaType.Json, ImmutableDictionary<string, string>.Empty);

    public TMessage Decode(ReadOnlySequence<byte> bytes, byte[]? schemaVersion = null)
    {
        try
        {
            var jsonString = Encoding.UTF8.GetString(bytes.ToArray());
            TMessage? message =
                JsonSerializer.Deserialize<TMessage>(jsonString);
            return message;
        }
        catch (Exception e)
        {
            throw e;
        }
    }

    public ReadOnlySequence<byte> Encode(TMessage message)
    {
        var jsonString = JsonSerializer.Serialize(message);
        return new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes(jsonString));
    }

    public SchemaInfo SchemaInfo { get; }
}
