using System.Reflection;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using MirrorPulse.Adapter.WebDav.Tests;

if (args.Length != 2 || args[0] != "--worker") return 2;
string executable = Path.GetFullPath(args[1]);
if (!File.Exists(executable)) return 2;
Environment.SetEnvironmentVariable("MP_WEBDAV_TEST_WORKER_EXE", executable);
int count = 0;
foreach (Type type in new[] { typeof(WebDavWorkerProcessTests), typeof(WebDavWorkerMutationTests) })
{
    object fixture = Activator.CreateInstance(type)!;
    foreach (MethodInfo method in type.GetMethods().Where(method => method.GetCustomAttribute<TestMethodAttribute>() is not null).OrderBy(method => method.Name, StringComparer.Ordinal))
    {
        await (Task)method.Invoke(fixture, null)!;
        count++;
        Console.WriteLine("Passed: " + method.Name);
    }
}
if (count != 13) throw new InvalidDataException("The complete WebDAV conformance profile must execute without skips.");
Console.WriteLine("WebDAV conformance passed with the Worker private runtime: " + count);
return 0;
