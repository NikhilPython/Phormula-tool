import { createSlice, PayloadAction } from "@reduxjs/toolkit";
import type { AmazonConnection } from "@/lib/utils/useAmazonConnections";

type AmazonState = {
  connections: AmazonConnection[];
  selectedRegion: string | null; // e.g. "eu-west-1"
};

const initialState: AmazonState = {
  connections: [],
  selectedRegion: null,
};

const amazonSlice = createSlice({
  name: "amazon",
  initialState,
  reducers: {
    setConnections(state, action: PayloadAction<AmazonConnection[]>) {
      state.connections = action.payload;

      // if nothing selected yet, default to first region
      if (!state.selectedRegion && action.payload.length > 0) {
        state.selectedRegion = action.payload[0].region;
      }
    },
    setSelectedRegion(state, action: PayloadAction<string | null>) {
      state.selectedRegion = action.payload;
    },
  },
});

export const { setConnections, setSelectedRegion } = amazonSlice.actions;
export default amazonSlice.reducer;
