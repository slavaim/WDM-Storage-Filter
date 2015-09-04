
/*
Copyright (c) 2008 Slava Imameev, All Rights Reserved

Revision history:
02.09.2008 ( September )
 Initial version

No any part of this code can be used in a commercial project.
*/

#include "stdafx.h"
#include <windows.h>
#include <devguid.h>
#include <Rpc.h>
#include "..\PctDriver\PctKernelToUser.h"

#define INITGUID
#include <guiddef.h>
#include <devguid.h>

SC_HANDLE
InitializeAndStartDriver(
    __in PWCHAR    PnpFilterServiceName,
    __in PWCHAR    FilePath,
    __in PWCHAR    Description
    )
    /*
    */
{
    BOOLEAN      RetValue = FALSE;
    SC_HANDLE    SCManagerHandle;
    SC_HANDLE    ServiceHandle = NULL;

    wprintf( L"Openining SCM...\n" );

    SCManagerHandle = OpenSCManager( NULL,
                                     NULL,
                                     SC_MANAGER_CREATE_SERVICE );

    if( NULL != SCManagerHandle )
    {

        wprintf( L"The operation has completed successfully\n" );
        wprintf( L"Creating or opening the %s service...\n", PnpFilterServiceName );

        ServiceHandle = CreateService( SCManagerHandle,
                                       PnpFilterServiceName,
                                       Description, 
                                       SERVICE_START | DELETE | SERVICE_STOP, 
                                       SERVICE_KERNEL_DRIVER,
                                       SERVICE_DEMAND_START, 
                                       SERVICE_ERROR_IGNORE, 
                                       FilePath, 
                                       NULL, NULL, NULL, NULL, NULL);
        if( NULL == ServiceHandle )
        {
            ServiceHandle = OpenService( SCManagerHandle,
                                         PnpFilterServiceName,
                                         SERVICE_START | DELETE | SERVICE_STOP );
        }

        if( NULL != ServiceHandle )
        {

            wprintf( L"Loading the %s driver ... \n", PnpFilterServiceName );

            if( StartService( ServiceHandle, 0, NULL) )
            {
                wprintf( L"The operation has completed successfully\n" );
                RetValue = TRUE;
            }
            else
            {
                ULONG error = GetLastError();
                if( ERROR_SERVICE_ALREADY_RUNNING == error ){
                    wprintf( L"The operation has completed successfully\n" );
                    RetValue = TRUE;
                } else {
                    wprintf( L"The %s driver loading failed with the error %i\n", PnpFilterServiceName, GetLastError() );
                }
            }

        }
        else
        {
            wprintf( L"The %s service opening or creating failed with the error %i\n", PnpFilterServiceName, GetLastError() );
        }

    }
    else
    {
        wprintf( L"The Service Control Manager opening failed with the error %i\n", GetLastError() );
    }

    return ServiceHandle;
}

int _tmain(int argc, _TCHAR* argv[])
{
    HANDLE            driver;
    PCT_IO_REQUEST    request = { 0x0 };
    int               bufsize = 2*0x1000;
    void*             buffer;
    DWORD             bytesSent;
    OVERLAPPED        Overlap = { 0x0 };
    SC_HANDLE         ServiceHandle;
    SERVICE_STATUS    ServiceStatus;

    buffer = malloc( bufsize );
    if( !buffer )
        return 1;

    ServiceHandle = InitializeAndStartDriver( L"PctDriver", L"C:\\pctdriver.sys", L"test driver" );
    if( NULL == ServiceHandle )
        return 1;

    driver = CreateFile( L"\\\\.\\PctCommunicationObject\\Device\\Harddisk0\\DR0",
                         GENERIC_ALL, 0, NULL, OPEN_EXISTING, 0x0/*FILE_FLAG_OVERLAPPED*/, NULL);
    if( INVALID_HANDLE_VALUE == driver ){

            wprintf( L"Can't open driver %u\n", GetLastError() );
            return 1;
    }

    request.ReadOperation = 0x1;

    request.UserBuffer = (PVOID)( ((ULONG_PTR)buffer+0x1000-1)&( ~0x1000+1 ) );
    request.BufferSize = 0x1000;

    request.OffsetInSectors.QuadPart = 0x0;
    request.NumberOfSectors = 0x1;

    DeviceIoControl( driver,
                     PCT_IOCTL_IO_REQUEST,
                     &request, sizeof( request ),
                     NULL, 0x0, //&request, sizeof( request ),
                     &bytesSent, NULL/*&Overlap*/ );

    WaitForSingleObject( driver, INFINITE );
    CloseHandle( driver );

    //wprintf( L"number of bytes read or written 0x%X\n", request.BytesReadOrWritten );

    free( buffer );

    //
    // unload
    //
    //ControlService( ServiceHandle, SERVICE_CONTROL_STOP, &ServiceStatus );
    CloseServiceHandle( ServiceHandle );

    return 0;
}

