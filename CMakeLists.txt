cmake_minimum_required(VERSION 3.22)
project(CHENGZHANG_DATA)

set(CMAKE_CXX_STANDARD 17)

add_executable(CHENGZHANG_DATA main.cpp lock_manager.h lock_manager.cpp transaction_manager.h transaction_manager.cpp rwlatch.h)
