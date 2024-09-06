import pydicom


def shared_functional_group_sequence(dataset, x):
    """
    Create the shared functional group sequence in the dataset.

    This function constructs the shared functional group sequence for a DICOM dataset,
    populating it with anatomical, reference, orientation, and pixel measurement information.

    Args:
        dataset (pydicom.Dataset): The DICOM dataset to which the functional groups are added.
        x (list): List containing data for constructing the functional groups.
    """
    anatomy_region_seq = pydicom.Sequence()
    anatomic_region_item = pydicom.Dataset()
    anatomic_region_item.CodeValue = ["5665001"]
    anatomic_region_item.CodingSchemeDesignator = ["SCT"]
    anatomic_region_item.CodeMeaning = ["Retina"]
    anatomy_region_seq.append(anatomic_region_item)

    frame_anatomic_seq = pydicom.Sequence()
    frame_anatomic_item = pydicom.Dataset()
    frame_anatomic_item.AnatomicRegionSequence = anatomy_region_seq
    frame_anatomic_item.FrameLaterality = (
        x[0]["52009229"].value[0]["00209071"].value[0]["00209072"].value
    )
    frame_anatomic_seq.append(frame_anatomic_item)

    purpose_of_reference_seq = pydicom.Sequence()
    purpose_of_reference_item = pydicom.Dataset()
    purpose_of_reference_item.CodeValue = ["121311"]
    purpose_of_reference_item.CodingSchemeDesignator = ["DCM"]
    purpose_of_reference_item.CodeMeaning = ["Localizer"]
    purpose_of_reference_seq.append(purpose_of_reference_item)

    referenced_image_seq = pydicom.Sequence()
    referenced_image_item = pydicom.Dataset()
    referenced_image_item.ReferencedSOPClassUID = (
        x[0]["52009229"].value[0]["00081140"].value[0]["00081150"].value
    )
    referenced_image_item.ReferencedSOPInstanceUID = (
        x[0]["52009229"].value[0]["00081140"].value[0]["00081155"].value
    )
    referenced_image_item.PurposeOfReferenceCodeSequence = purpose_of_reference_seq
    referenced_image_seq.append(referenced_image_item)

    derivation_image_seq = pydicom.Sequence()

    plane_orientation_seq = pydicom.Sequence()
    plane_orientation_item = pydicom.Dataset()
    plane_orientation_item.ImageOrientationPatient = (
        x[0]["52009229"].value[0]["00209116"].value[0]["00200037"].value
    )
    plane_orientation_seq.append(plane_orientation_item)

    pixel_measures_seq = pydicom.Sequence()
    pixel_measures_item = pydicom.Dataset()
    pixel_measures_item.SliceThickness = (
        x[0]["52009229"].value[0]["00289110"].value[0]["00180050"].value
    )
    pixel_measures_item.PixelSpacing = (
        x[0]["52009229"].value[0]["00289110"].value[0]["00280030"].value
    )
    pixel_measures_seq.append(pixel_measures_item)

    shared_func_groups_seq = pydicom.Sequence()
    dataset.SharedFunctionalGroupsSequence = shared_func_groups_seq
    shared_func_item = pydicom.Dataset()
    shared_func_item.FrameAnatomySequence = frame_anatomic_seq
    shared_func_item.ReferencedImageSequence = referenced_image_seq
    shared_func_item.PlaneOrientationSequence = plane_orientation_seq
    shared_func_item.PixelMeasuresSequence = pixel_measures_seq

    shared_func_item.DerivationImageSequence = derivation_image_seq

    shared_func_groups_seq.append(shared_func_item)


def per_frame_functional_groups_sequence(dataset, x):
    """
    Create the per-frame functional groups sequence in the dataset.

    This function constructs the per-frame functional groups sequence for a DICOM dataset,
    populating it with per-frame content, reference, ophthalmic, and position information.

    Args:
        dataset (pydicom.Dataset): The DICOM dataset to which the functional groups are added.
        x (list): List containing data for constructing the functional groups.
    """
    per_frame_functional_groups_seq = pydicom.Sequence()
    repeat = len(x[0]["52009230"].value)

    for i in range(repeat):
        frame_content_seq = pydicom.Sequence()
        frame_content_item = pydicom.Dataset()
        frame_content_item.FrameAcquisitionDateTime = (
            x[0]["52009230"].value[i]["00209111"].value[0]["00189074"].value
        )
        frame_content_item.FrameReferenceDateTime = (
            x[0]["52009230"].value[i]["00209111"].value[0]["00189151"].value
        )
        frame_content_item.FrameAcquisitionDuration = (
            x[0]["52009230"].value[i]["00209111"].value[0]["00189220"].value
        )
        frame_content_item.StackID = (
            x[0]["52009230"].value[i]["00209111"].value[0]["00209056"].value
        )
        frame_content_item.InStackPositionNumber = (
            x[0]["52009230"].value[i]["00209111"].value[0]["00209057"].value
        )
        frame_content_item.DimensionIndexValues = (
            x[0]["52009230"].value[i]["00209111"].value[0]["00209157"].value
        )
        frame_content_seq.append(frame_content_item)

        purpose_of_reference_seq = pydicom.Sequence()
        purpose_of_reference_item = pydicom.Dataset()
        purpose_of_reference_item.CodeValue = ["121311"]
        purpose_of_reference_item.CodingSchemeDesignator = ["DCM"]
        purpose_of_reference_item.CodeMeaning = ["Localizer"]
        purpose_of_reference_seq.append(purpose_of_reference_item)

        ophthalmic_frame_location_seq = pydicom.Sequence()
        ophthalmic_frame_location_item = pydicom.Dataset()
        ophthalmic_frame_location_item.ReferencedSOPClassUID = (
            x[0]["52009230"].value[i]["00220031"].value[0]["00081150"].value
        )
        ophthalmic_frame_location_item.ReferencedSOPInstanceUID = (
            x[0]["52009230"].value[i]["00220031"].value[0]["00081155"].value
        )
        ophthalmic_frame_location_item.ReferenceCoordinates = (
            x[0]["52009230"].value[i]["00220031"].value[0]["00220032"].value
        )
        ophthalmic_frame_location_item.OphthalmicImageOrientation = (
            x[0]["52009230"].value[i]["00220031"].value[0]["00220039"].value
        )
        ophthalmic_frame_location_item.PurposeOfReferenceCodeSequence = (
            purpose_of_reference_seq
        )
        ophthalmic_frame_location_seq.append(ophthalmic_frame_location_item)

        plane_position_seq = pydicom.Sequence()
        plane_position_item = pydicom.Dataset()
        try:
            value = x[0]["52009230"].value[i]["00209113"].value[0]["00200032"].value
        except KeyError:
            value = []
        plane_position_item.ImagePositionPatient = value
        plane_position_seq.append(plane_position_item)

        per_frame_functional_groups_item = pydicom.Dataset()
        per_frame_functional_groups_item.FrameContentSequence = frame_content_seq
        per_frame_functional_groups_item.PlanePositionSequence = plane_position_seq
        per_frame_functional_groups_item.OphthalmicFrameLocationSequence = (
            ophthalmic_frame_location_seq
        )
        per_frame_functional_groups_seq.append(per_frame_functional_groups_item)

    dataset.PerFrameFunctionalGroupsSequence = per_frame_functional_groups_seq


def acquisition_device_type_code_sequence(dataset, x):
    """
    Create the acquisition device type code sequence in the dataset.

    This function constructs the acquisition device type code sequence for a DICOM dataset,
    specifying the type of device used for acquisition.

    Args:
        dataset (pydicom.Dataset): The DICOM dataset to which the acquisition device type code sequence is added.
        x (list): List containing data for constructing the sequence.
    """

    acquisition_device_type_code_seq = pydicom.Sequence()
    acquisition_device_type_code_item = pydicom.Dataset()

    acquisition_device_type_code_item.CodeValue = "392012008"
    acquisition_device_type_code_item.CodingSchemeDesignator = "SCT"
    acquisition_device_type_code_item.CodeMeaning = (
        "Optical Coherence Tomography Scanner"
    )

    acquisition_device_type_code_seq.append(acquisition_device_type_code_item)

    dataset.AcquisitionDeviceTypeCodeSequence = acquisition_device_type_code_seq


def anatomic_region_sequence(dataset, x):
    """
    Create the anatomic region sequence in the dataset.

    This function constructs the anatomic region sequence for a DICOM dataset,
    specifying the anatomical region imaged.

    Args:
        dataset (pydicom.Dataset): The DICOM dataset to which the anatomic region sequence is added.
        x (list): List containing data for constructing the sequence.
    """

    anatomic_region_seq = pydicom.Sequence()
    anatomic_region_item = pydicom.Dataset()

    anatomic_region_item.CodeValue = "5665001"
    anatomic_region_item.CodingSchemeDesignator = "SCT"
    anatomic_region_item.CodeMeaning = "Retina"

    anatomic_region_seq.append(anatomic_region_item)

    dataset.AnatomicRegionSequence = anatomic_region_seq


def dimension_organization_sequence(dataset, x):
    """
    Create the dimension organization sequence in the dataset.

    This function constructs the dimension organization sequence for a DICOM dataset,
    specifying the organization of dimensions in the dataset.

    Args:
        dataset (pydicom.Dataset): The DICOM dataset to which the dimension organization sequence is added.
        x (list): List containing data for constructing the sequence.
    """

    dimension_organization_seq = pydicom.Sequence()
    dimension_organization_item = pydicom.Dataset()

    dimension_organization_item.DimensionOrganizationUID = (
        x[0]["00209221"].value[0]["00209164"].value
    )

    dimension_organization_seq.append(dimension_organization_item)

    dataset.DimensionOrganizationSequence = dimension_organization_seq


def dimension_index_sequence(dataset, x):
    """
    Create the dimension index sequence in the dataset.

    This function constructs the dimension index sequence for a DICOM dataset,
    specifying the indices of dimensions in the dataset.

    Args:
        dataset (pydicom.Dataset): The DICOM dataset to which the dimension index sequence is added.
        x (list): List containing data for constructing the sequence.
    """

    dimension_index_seq = pydicom.Sequence()
    dimension_index_item = pydicom.Dataset()

    dimension_index_item.DimensionOrganizationUID = (
        x[0]["00209221"].value[0]["00209164"].value
    )
    dimension_index_item.DimensionIndexPointer = "00209056"
    dimension_index_item.FunctionalGroupPointer = "00209111"

    dimension_index_seq.append(dimension_index_item)

    dimension_index_item1 = pydicom.Dataset()

    dimension_index_item1.DimensionOrganizationUID = (
        x[0]["00209221"].value[0]["00209164"].value
    )
    dimension_index_item1.DimensionIndexPointer = "00209057"
    dimension_index_item1.FunctionalGroupPointer = "00209111"

    dimension_index_seq.append(dimension_index_item1)

    dataset.DimensionIndexSequence = dimension_index_seq
