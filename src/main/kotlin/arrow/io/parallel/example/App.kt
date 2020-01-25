package arrow.io.parallel.example

import arrow.core.Either
import arrow.fx.IO
import arrow.fx.extensions.fx
import arrow.fx.extensions.io.concurrent.parMapN
import arrow.fx.extensions.io.dispatchers.dispatchers
import kotlin.system.measureTimeMillis

fun getFileContent(file: String) = IO<String> {
    Thread.sleep(2000)
    java.io.File(file).readText()
}

fun saveFile(file: String, content: String) = IO<Unit> {
    java.io.File(file).writeText(content)
}

fun combineFilesSequential(files: List<String>, destinationFile: String) = IO.fx {
    val part1 = !getFileContent(files[0])
    val part2 = !getFileContent(files[1])
    val part3 = !getFileContent(files[2])
    val part4 = !getFileContent(files[3])
    val full = part1 + part2 + part3 + part4
    !saveFile(destinationFile, full)
    full
}

fun combineFilesParMapN(files: List<String>) = IO.dispatchers().default().parMapN(
        getFileContent(files[0]),
        getFileContent(files[1]),
        getFileContent(files[2]),
        getFileContent(files[3])
) { a, b, c, d ->
    a + b + c + d
}

fun combineFilesParallelTraverse(files: List<String>, destinationFile: String) = IO.fx {
    val contentLines = !files.parTraverse {
        getFileContent(it)
    }
    val fullContent = contentLines.joinToString("")
    !saveFile(destinationFile, fullContent)
    fullContent
}

fun main(args: Array<String>) {
    val fileList = listOf(
            "data/part1.txt",
            "data/part2.txt",
            "data/part3.txt",
            "data/part4.txt"
    )

    val sequentialTime = measureTimeMillis {
        // Sequential load. Takes around 8+ seconds.
        combineFilesSequential(fileList, "data/full-sequential.txt").attempt().map {
            when (it) {
                is Either.Left -> println("Could not load files.")
                is Either.Right -> println(it.b)
            }
        }.unsafeRunSync()
    }

    println("Sequential Time: $sequentialTime\n")

    val parTraverseTime = measureTimeMillis {
        // Parallel load. Takes around 2+ seconds.
        combineFilesParallelTraverse(fileList, "data/full-parallel.txt").attempt().map {
            when (it) {
                is Either.Left -> println("Could not load all files.")
                is Either.Right -> println(it.b)
            }
        }.unsafeRunSync()
    }

    println("ParTraverse Time: $parTraverseTime\n")
}
