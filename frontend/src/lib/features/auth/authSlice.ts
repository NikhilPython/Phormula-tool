// src/lib/features/auth/authSlice.ts
import { createSlice, PayloadAction, isAnyOf } from "@reduxjs/toolkit";
import type { User } from "@/lib/api/userApi";
import { authApi } from "@/lib/api/authApi";
import { userApi } from "@/lib/api/userApi"; // make sure this exports getUser

export type AuthState = {
  token: string | null;
  user: User | null;
  status: "idle" | "loading" | "succeeded" | "failed";
  error?: string;
};

const initialState: AuthState = {
  token: typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null,
  user: null,
  status: "idle",
};

const authSlice = createSlice({
  name: "auth",
  initialState,
  reducers: {
    setCredentials(state, action: PayloadAction<{ token: string }>) {
      state.token = action.payload.token;
      state.status = "succeeded";
      if (typeof window !== "undefined") {
        localStorage.setItem("jwtToken", action.payload.token);
      }
    },
    setUser(state, action: PayloadAction<Partial<User> | null>) {
      if (action.payload === null) {
        state.user = null;
      } else {
        state.user = { ...(state.user ?? {}), ...action.payload };
      }
    },

    setAuthLoading(state) {
      state.status = "loading";
      state.error = undefined;
    },
    setAuthError(state, action: PayloadAction<string | undefined>) {
      state.status = "failed";
      state.error = action.payload || "Authentication failed";
    },
    logout(state) {
      state.token = null;
      state.user = null;
      state.status = "idle";
      state.error = undefined;
      if (typeof window !== "undefined") {
        localStorage.removeItem("jwtToken");
        localStorage.removeItem("email");
        localStorage.removeItem("password");
      }
    },
  },
  extraReducers: (builder) => {
    // ---- LOGIN (authApi.login) ----
    builder.addMatcher(authApi.endpoints.login.matchPending, (state) => {
      state.status = "loading";
      state.error = undefined;
    });

    builder.addMatcher(authApi.endpoints.login.matchFulfilled, (state, { payload }) => {
      // payload: { token, message? }
      state.status = "succeeded";
      state.error = undefined;
      state.token = payload?.token ?? null;
      if (typeof window !== "undefined" && payload?.token) {
        localStorage.setItem("jwtToken", payload.token);
      }
    });

    builder.addMatcher(authApi.endpoints.login.matchRejected, (state, action: any) => {
      state.status = "failed";
      // Prefer server message if present
      const msg =
        action?.payload?.data?.message ||
        action?.error?.data?.message ||
        action?.error?.message ||
        "Login failed";
      state.error = msg;
    });

    // ---- REGISTER (authApi.register) ----
    builder.addMatcher(authApi.endpoints.register.matchPending, (state) => {
      state.status = "loading";
      state.error = undefined;
    });

    builder.addMatcher(authApi.endpoints.register.matchFulfilled, (state) => {
      // NOTE: No token set on register; verification needed
      state.status = "succeeded";
      state.error = undefined;
    });

    builder.addMatcher(authApi.endpoints.register.matchRejected, (state, action: any) => {
      state.status = "failed";
      const msg =
        action?.payload?.data?.message ||
        action?.error?.data?.message ||
        action?.error?.message ||
        "Registration failed";
      state.error = msg;
    });

    // ---- HYDRATE USER (userApi.getUser) ----
    builder.addMatcher(userApi.endpoints.getUser.matchFulfilled, (state, { payload }) => {
      state.user = payload || null;
      // don't change status here; this runs on many pages
    });

    // (Optional) If you want to reflect loading while fetching user:
    // builder.addMatcher(userApi.endpoints.getUser.matchPending, (state) => {
    //   if (!state.user) state.status = "loading";
    // });
    // builder.addMatcher(userApi.endpoints.getUser.matchRejected, (state) => {
    //   if (!state.user) state.status = "failed";
    // });
  },
});

export const { setCredentials, setUser, setAuthLoading, setAuthError, logout } =
  authSlice.actions;

export default authSlice.reducer;

/* ========== Selectors (unchanged shape) ========== */
export const selectAuth = (s: { auth: AuthState }) => s.auth;
export const selectIsAuthenticated = (s: { auth: AuthState }) => !!s.auth.token;
