using System.Buffers;
using System.Buffers.Text;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO.Pipelines;
using System.Net.Sockets;
using System.Text;

var listener = new TcpListener(System.Net.IPAddress.Any, 6379);
listener.Start();

var state = new ConcurrentDictionary<string, string>();

while (true)
{
    var tcp = listener.AcceptTcpClient();
    new Client(state, tcp).HandleConnection()
        .ContinueWith(t =>
        {
            using (tcp)
            {
                if (t.Exception != null)
                    Console.WriteLine(t.Exception);
            }
        });
}


public class Client
{
    ConcurrentDictionary<string, string> _state;
    PipeReader _netReader;
    PipeWriter _netWriter;
    List<ReadOnlySequence<byte>> _cmds = new();
    byte[] _temp = new byte[4];

    public Client(ConcurrentDictionary<string, string> state, TcpClient tcp)
    {
        _state = state;
        var stream = tcp.GetStream();
        _netReader = PipeReader.Create(stream);
        _netWriter = PipeWriter.Create(stream);

    }

    public async Task HandleConnection()
    {
        while (true)
        {
            var result = await _netReader.ReadAsync();
            var (consumed, examined) = ParseNetworkData(result);
            _netReader.AdvanceTo(consumed, examined);
            await _netWriter.FlushAsync();
        }
    }


    (SequencePosition Consumed, SequencePosition Examined) ParseNetworkData(ReadResult result)
    {
        var reader = new SequenceReader<byte>(result.Buffer);
        SequencePosition consumed;
        while (true)
        {
            _cmds.Clear();

            consumed = reader.Position;
            if (reader.TryReadTo(out ReadOnlySpan<byte> line, (byte)'\n') == false)
                return (consumed, result.Buffer.End);

            if (line.Length == 0 || line[0] != '*' || line[line.Length - 1] != '\r')
                ThrowBadBuffer(result.Buffer);
            if (Utf8Parser.TryParse(line.Slice(1), out int argc, out int bytesConsumed) == false ||
                bytesConsumed + 2 != line.Length) // account for the * and \r
                ThrowBadBuffer(result.Buffer);

            for (int i = 0; i < argc; i++)
            {
                if (reader.TryReadTo(out line, (byte)'\n') == false)
                {
                    return (consumed, result.Buffer.End);
                }

                if (line.Length == 0 || line[0] != '$' || line[line.Length - 1] != '\r')
                    ThrowBadBuffer(result.Buffer);
                if (Utf8Parser.TryParse(line.Slice(1), out int size, out bytesConsumed) == false ||
                    bytesConsumed + 2 != line.Length) // accounts for $ and \r
                    ThrowBadBuffer(result.Buffer);

                if (size + 2 /*\r\n*/ > reader.UnreadSequence.Length)
                {
                    return (consumed, result.Buffer.End);
                }

                var arg = reader.UnreadSequence.Slice(0, size);
                _cmds.Add(arg);
                reader.Advance(size);
                if (reader.TryReadTo(out line, (byte)'\n') == false)
                {
                    return (consumed, result.Buffer.End);
                }


                if (line.Length == 0 || line[0] != '\r')
                    ThrowBadBuffer(result.Buffer);
            }

            ExecCommand(_cmds);
        }
    }

    private void ExecCommand(List<ReadOnlySequence<byte>> cmds)
    {
        if (cmds[0].Length != 3)
            ThrowBadBuffer(cmds[0]);
        ReadOnlySpan<byte> cmd;
        if (cmds[0].IsSingleSegment == false)
        {
            cmds[0].CopyTo(_temp);
            cmd = new ReadOnlySpan<byte>(_temp, 0, 3);
        }
        else
        {
            cmd = cmds[0].FirstSpan;
        }

        if (cmd[1] != (byte)'E' || cmd[2] != (byte)'T')
            ThrowBadBuffer(cmds[0]);

        var key = Encoding.UTF8.GetString(cmds[1]);
        if (cmd[0] == (byte)'G')
        {
            if (_state.TryGetValue(key, out var result))
            {
                var len = Encoding.UTF8.GetByteCount(result);
                var mem = _netWriter.GetMemory(len + 16); // \r\n times 2 + result length
                mem.Span[0] = (byte)'$';
                if (Utf8Formatter.TryFormat(len, mem.Span.Slice(1), out var written) == false)
                    ThrowImpossibleFailedWrite();
                written += 1; // for the '$'
                written += WriteEndOfLine(mem.Span, written);
                written += Encoding.UTF8.GetBytes(result, mem.Span.Slice(written));
                written += WriteEndOfLine(mem.Span, written);
                _netWriter.Advance(written);
            }
            else
            {
                WriteMissing();
            }
        }
        else if (cmd[0] == (byte)'S')
        {
            var val = Encoding.UTF8.GetString(cmds[2]);
            _state[key] = val;
            WriteMissing();
        }
        else
        {
            ThrowBadBuffer(cmds[0]);
        }
    }

    private int WriteEndOfLine(Span<byte> span, int offset)
    {
        span[offset] = (byte)'\r';
        span[offset + 1] = (byte)'\n';
        return 2;
    }

    private void WriteMissing()
    {
        var span = _netWriter.GetMemory(5).Span;

        span[0] = (byte)'$';
        span[1] = (byte)'-';
        span[2] = (byte)'1';
        span[3] = (byte)'\r';
        span[4] = (byte)'\n';

        _netWriter.Advance(5);

    }

    private static void ThrowImpossibleFailedWrite()
    {
        throw new InvalidOperationException("Unable to write to memory, impopssible");
    }

    void ThrowBadBuffer(ReadOnlySequence<byte> buf)
    {
        throw new InvalidDataException("The buffer didn't match the expected value: " + Encoding.UTF8.GetString(buf));
    }
}
