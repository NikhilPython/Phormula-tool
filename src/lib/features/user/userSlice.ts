import { createSlice, PayloadAction } from '@reduxjs/toolkit'

type UserState = {
  name: string
  email: string
}

const initialState: UserState = {
  name: '',
  email: '',
}

const userSlice = createSlice({
  name: 'user',
  initialState,
  reducers: {
    setUser: (state, action: PayloadAction<{ name: string; email: string }>) => {
      state.name = action.payload.name
      state.email = action.payload.email
    },
    clearUser: (state) => {
      state.name = ''
      state.email = ''
    },
  },
})

export const { setUser, clearUser } = userSlice.actions
export default userSlice.reducer
