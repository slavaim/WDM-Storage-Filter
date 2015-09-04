/*
Copyright (c) 2008 Slava Imameev, All Rights Reserved

Revision history:
02.09.2008 ( September )
 Initial version

No any part of this file can be used in a commercial project.
*/
#ifndef _PCT_KERNEL_TO_USER_H_
#define _PCT_KERNEL_TO_USER_H_

//
// {78987375-4D98-4498-A50E-A88CF67F6A16}
//
DEFINE_GUID(GUID_PCT_CONTROL_OBJECT, 
0x78987375, 0x4d98, 0x4498, 0xa5, 0xe, 0xa8, 0x8c, 0xf6, 0x7f, 0x6a, 0x16);

#ifdef __cplusplus
extern "C" {
#endif//__cplusplus

#define PCT_FULL_COMM_OBJECT_NAME    L"\\Device\\PctCommunicationObject"
#define PCT_DEVICE_SYMBOLIC_NAME     L"\\DosDevices\\PctCommunicationObject"

#define FILE_DEVICE_PCTL    0x00008779

#define PCT_IOCTL_IO_REQUEST    CTL_CODE( FILE_DEVICE_PCTL, 0, METHOD_BUFFERED, FILE_ANY_ACCESS )

typedef struct _PCT_IO_REQUEST{

    LARGE_INTEGER    OffsetInSectors;
    ULONG            NumberOfSectors;

    PVOID            UserBuffer;
    ULONG            BufferSize;

    ULONG            BytesReadOrWritten;

    ULONG            ReadOperation: 0x1;
    ULONG            WriteOperation: 0x1;

} PCT_IO_REQUEST, *PPCT_IO_REQUEST;


#ifdef __cplusplus
}
#endif//__cplusplus

#endif//_PCT_KERNEL_TO_USER_H_
