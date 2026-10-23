"use client";

import { useEffect } from "react";

let isInstalled = false;
let isRedirecting = false;

export default function Global401Interceptor() {
  useEffect(() => {
    if (isInstalled) return;
    isInstalled = true;

    const originalFetch = window.fetch;
    const originalXHROpen = XMLHttpRequest.prototype.open;
    const originalXHRSend = XMLHttpRequest.prototype.send;

    const apiBase = process.env.NEXT_PUBLIC_API_BASE_URL;

    if (!apiBase) {
      console.warn("Global401Interceptor: API base URL not configured");
      isInstalled = false;
      return;
    }

    const apiOrigin = new URL(apiBase, window.location.origin);

    const isBackendRequest = (url: string) => {
      try {
        const requestUrl = new URL(url, window.location.origin);

        const basePath = apiOrigin.pathname.replace(/\/$/, "");

        return (
          requestUrl.origin === apiOrigin.origin &&
          (
            !basePath ||
            requestUrl.pathname === basePath ||
            requestUrl.pathname.startsWith(`${basePath}/`)
          )
        );
      } catch {
        return false;
      }
    };

    const handleUnauthorized = (url: string, status: number) => {
      if (status !== 401 || isRedirecting) return;
      if (!isBackendRequest(url)) return;

      // Do not intercept public authentication requests
      const path = new URL(url, window.location.origin).pathname;

      if (
        path === "/signin" ||
        path === "/login" ||
        path === "/register" ||
        path === "/api/auth/login" ||
        path === "/api/auth/refresh"
      ) {
        return;
      }

      if (
        window.location.pathname === "/signin" ||
        window.location.pathname === "/signup"
      ) {
        return;
      }

      if (!localStorage.getItem("jwtToken")) return;

      isRedirecting = true;

      localStorage.removeItem("jwtToken");

      // Clear any additional auth keys your application uses.
      // Clear auth cookies through your logout endpoint if HttpOnly.

      window.location.replace("/signin");
    };

    // Intercept fetch() and RTK Query fetchBaseQuery
    window.fetch = async (...args) => {
      const response = await originalFetch.apply(window, args);

      const request = args[0];

      const url =
        typeof request === "string"
          ? request
          : request instanceof URL
            ? request.href
            : request.url;

      handleUnauthorized(url, response.status);

      return response;
    };

    // Intercept XMLHttpRequest (used by some Axios configurations)
    const requestUrls = new WeakMap<XMLHttpRequest, string>();

    XMLHttpRequest.prototype.open = function (
      method: string,
      url: string | URL,
      ...rest: any[]
    ) {
      requestUrls.set(this, String(url));

      return originalXHROpen.apply(this, [
        method,
        url,
        ...rest,
      ] as Parameters<typeof originalXHROpen>);
    };

    XMLHttpRequest.prototype.send = function (body) {
      this.addEventListener("loadend", () => {
        const url = requestUrls.get(this);

        if (url) {
          handleUnauthorized(url, this.status);
        }
      });

      return originalXHRSend.call(this, body);
    };

    // Restore interceptors on unmount
    return () => {
      window.fetch = originalFetch;
      XMLHttpRequest.prototype.open = originalXHROpen;
      XMLHttpRequest.prototype.send = originalXHRSend;
      isInstalled = false;
    };
  }, []);

  return null;
}