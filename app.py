import streamlit as st
import model as ab

# Define your Streamlit app
def main():
    st.title("Experiment Configuration")

    # Define inputs for the Experiment object
    exp_os_options = ['Android', 'iOS']
    exp_os = st.selectbox("Experiment OS", options=exp_os_options, index=1)
    exp_toggle = st.text_input("Experiment Toggle", value='firebase_exp_340')
    exp_name = st.text_input("Experiment Name", value='exp_filter_discount')
    start_date = st.date_input("Start Date", value=None)
    end_date = st.date_input("End Date", value=None)
    test_params = st.text_input("Test Parameters", value='test')

    if "exp" not in st.session_state:
        st.session_state.exp = None  # Initialize exp as None

    if "df" not in st.session_state:
        st.session_state.df = None

    if "pub_res_df" not in st.session_state:
        st.session_state.pub_res_df = None

    # Create the Experiment object when the button is clicked
    if st.button("Initialize Experiment") or st.session_state.exp is not None:
        exp = ab.Experiment(
            exp_os=exp_os,
            exp_toggle=exp_toggle,
            exp_name=exp_name,
            start_date=start_date.strftime('%Y-%m-%d') if start_date else None,
            end_date=end_date.strftime('%Y-%m-%d') if end_date else None,
            test_params=test_params
        )

        # Store the initialized Experiment object in session state
        st.session_state.exp = exp

        # Display the initialized Experiment object
        st.write("Experiment Initialized:")
        st.write("Experiment OS:", exp.exp_os)
        st.write("Experiment Toggle:", exp.exp_toggle)
        st.write("Experiment Name:", exp.exp_name)
        st.write("Start Date:", exp.start_date)
        st.write("End Date:", exp.end_date)
        st.write("Test Parameters:", exp.test_params)

    st.title("Get data")
    
    if st.button("Get data from ClickHouse"):
        st.session_state.df = ab.pd.read_csv("ab_data.csv")  # Assigning value to df
        st.session_state.df.group_field = st.session_state.df.group_field.astype(str)

    # File upload for CSV after initializing the Experiment
    uploaded_file = st.file_uploader("Or upload CSV file", type=["csv"])
    if uploaded_file is not None:
        st.session_state.df = ab.pd.read_csv(uploaded_file)
        st.session_state.df = st.session_state.df.group_field.astype(str)

    # Display df.head(10) if it exists
    if st.session_state.df is not None:
        st.write("Uploaded Data:")
        st.write(st.session_state.df.head(10))

    st.title("Get results")

    if st.button("Get results"):
        if st.session_state.df is not None and st.session_state.exp is not None:
            res_df, res_pivot_df = ab.get_results(st.session_state.df)
            st.session_state.pub_res_df = ab.get_publish_results(st.session_state.exp, res_pivot_df)

    # Display pub_res_df if it exists
    if st.session_state.pub_res_df is not None:
        st.write("Results:")
        st.write(st.session_state.pub_res_df)

    if st.button("Save results"):
        if st.session_state.pub_res_df is not None and st.session_state.exp is not None:
            ab.save_results_to_excel(st.session_state.exp, st.session_state.pub_res_df)

# Run the app
if __name__ == "__main__":
    main()
