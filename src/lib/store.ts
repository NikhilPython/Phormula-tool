import { configureStore } from "@reduxjs/toolkit";
import authReducer from "./features/auth/authSlice";
import amazonReducer from "./api/amazonSlice";
import { baseApi } from "./api/baseApi";
import { useDispatch, useSelector, TypedUseSelectorHook } from "react-redux";
import { dashboardApi } from "./api/dashboardApi";


export const store = configureStore({
  reducer: {
    auth: authReducer,
    amazon: amazonReducer, // ✅ register slice here
    [baseApi.reducerPath]: baseApi.reducer,
    [dashboardApi.reducerPath]: dashboardApi.reducer,
  },
  middleware: (gDM) =>
    gDM().concat(baseApi.middleware).concat(dashboardApi.middleware),
  devTools: process.env.NODE_ENV !== "production",
});

export type RootState = ReturnType<typeof store.getState>;
export type AppDispatch = typeof store.dispatch;
export const useAppDispatch = () => useDispatch<AppDispatch>();
export const useAppSelector: TypedUseSelectorHook<RootState> = useSelector;
