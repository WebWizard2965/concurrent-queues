cmake_minimum_required(VERSION 3.16)
project(cpp-concurrency-lab LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(Threads REQUIRED)

function(add_lab_target name source)
    add_executable(${name} ${source})
    target_include_directories(${name} PRIVATE ${CMAKE_SOURCE_DIR}/include)
    target_compile_options(${name} PRIVATE -Wall -Wextra -Wpedantic)
    target_link_libraries(${name} PRIVATE Threads::Threads)
endfunction()

add_lab_target(producer_consumer src/producer_consumer.cpp)
add_lab_target(queue_stress tests/queue_stress.cpp)
add_lab_target(throughput bench/throughput.cpp)

target_compile_options(producer_consumer PRIVATE -O2)
target_compile_options(queue_stress PRIVATE -O2)
target_compile_options(throughput PRIVATE -O2)

add_lab_target(queue_stress_tsan tests/queue_stress.cpp)
target_compile_options(queue_stress_tsan PRIVATE -O1 -g -fsanitize=thread)
target_link_options(queue_stress_tsan PRIVATE -fsanitize=thread)
