自用脚本:
# cmd
- net localgroup Administrators Chuyin.Wang /add
- runas /user:amlogic\chuyin.wang "powershell.exe -NoExit -Command cd '%USERPROFILE%'"
# powershell
- wsl --mount "\\.\PHYSICALDRIVE1" --bare
# wsl-2
- sudo mount -o noatime,nodiratime,attr2,inode64,logbufs=8,logbsize=32k /dev/sde1 /mnt/dev/sde
