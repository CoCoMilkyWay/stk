// inject.exe — 在 Wine 里运行，把 shim.dll 注入到正在运行的 tdxw.exe。
// 经典三步：OpenProcess → VirtualAllocEx + WriteProcessMemory(写 dll 路径)
//          → CreateRemoteThread(LoadLibraryA)。
// Wine 9.0 支持这套跨进程
// API（NtAllocateVirtualMemory/NtWriteVirtualMemory/NtCreateThreadEx）。 32
// 位匹配 TdxW。编译见 build.sh。 用法：wine inject.exe <shim.dll 的 Windows
// 路径，如 Z:\home\chuyin\work\tdx\scripts\shim\shim.dll>

#include <stdio.h>
#include <string.h>
#include <windows.h>

#include <tlhelp32.h>

static DWORD find_proc(const char *name) {
  HANDLE snap = CreateToolhelp32Snapshot(TH32CS_SNAPPROCESS, 0);
  if (snap == INVALID_HANDLE_VALUE)
    return 0;
  PROCESSENTRY32 pe;
  pe.dwSize = sizeof(pe);
  DWORD pid = 0;
  if (Process32First(snap, &pe)) {
    do {
      if (!_stricmp(pe.szExeFile, name)) {
        pid = pe.th32ProcessID;
        break;
      }
    } while (Process32Next(snap, &pe));
  }
  CloseHandle(snap);
  return pid;
}

int main(int argc, char **argv) {
  const char *dll = argc > 1 ? argv[1] : "shim.dll";
  const char *proc = argc > 2 ? argv[2] : "tdxw.exe";
  DWORD pid = find_proc(proc);
  if (!pid) {
    printf("找不到 %s\n", proc);
    return 1;
  }
  printf("[*] %s pid=%lu\n", proc, pid);

  HMODULE k32 = GetModuleHandleA("kernel32.dll");
  LPVOID load = (LPVOID)GetProcAddress(k32, "LoadLibraryA");
  if (!load) {
    printf("拿不到 LoadLibraryA\n");
    return 2;
  }

  HANDLE hp = OpenProcess(PROCESS_ALL_ACCESS, FALSE, pid);
  if (!hp) {
    printf("OpenProcess 失败 err=%lu\n", GetLastError());
    return 3;
  }

  SIZE_T len = strlen(dll) + 1;
  LPVOID remote =
      VirtualAllocEx(hp, NULL, len, MEM_COMMIT | MEM_RESERVE, PAGE_READWRITE);
  if (!remote) {
    printf("VirtualAllocEx 失败 err=%lu\n", GetLastError());
    return 4;
  }

  SIZE_T written = 0;
  if (!WriteProcessMemory(hp, remote, dll, len, &written)) {
    printf("WriteProcessMemory 失败 err=%lu\n", GetLastError());
    return 5;
  }

  HANDLE th = CreateRemoteThread(hp, NULL, 0, (LPTHREAD_START_ROUTINE)load,
                                 remote, 0, NULL);
  if (!th) {
    printf("CreateRemoteThread 失败 err=%lu\n", GetLastError());
    return 6;
  }
  WaitForSingleObject(th, 5000);
  DWORD rc = 0;
  GetExitCodeThread(th, &rc);
  CloseHandle(th);
  VirtualFreeEx(hp, remote, 0, MEM_RELEASE);
  CloseHandle(hp);
  // LoadLibraryA 的返回值（模块句柄）就是远程线程退出码；0 表示加载失败。
  if (rc == 0) {
    printf("[!] LoadLibraryA 返回 0，DLL 未加载（路径/依赖问题）\n");
    return 7;
  }
  printf("[+] 注入成功，模块句柄=0x%lx，看 shim 是否在控制端口应答 ping\n", rc);
  return 0;
}
