package com.surajpanda.dmq.api;

/** A minimal, stable error body - never a raw stack trace. */
public record ApiError(String error, String message) {}
