import { initializeApp } from "firebase/app";
import { getAuth, GoogleAuthProvider } from "firebase/auth";

const firebaseConfig = {
  apiKey: "AIzaSyDAWXugpQBHFIKePbJFmBBxSF5XniOXnDE",
  authDomain: "phormula-8bd3c.firebaseapp.com",
  projectId: "phormula-8bd3c",
  storageBucket: "phormula-8bd3c.appspot.com",
  messagingSenderId: "271879745844",
  appId: "1:271879745844:web:4001d9a68c407517e7f561",
  measurementId: "G-TQ5ZQ3FMQ9",
};

export const app = initializeApp(firebaseConfig);
export const auth = getAuth(app);
export const googleProvider = new GoogleAuthProvider();
